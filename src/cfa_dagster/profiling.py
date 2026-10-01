from __future__ import annotations

import argparse
import base64
import logging
import os
import subprocess
import time
import zlib
from dataclasses import dataclass
from itertools import pairwise

import psutil
from dagster import Bool, Field, Float, Permissive

log = logging.getLogger(__name__)

DEFAULT_PROFILING_SAMPLE_INTERVAL_SECONDS = 1.0
PROFILING_CONFIG_KEY = "profiling"
PROFILER_SOURCE_ENV = "CFA_DAGSTER_PROFILER_SOURCE"
PROFILER_SOURCE_PSUTIL = "psutil"

PROFILING_CONFIG_SCHEMA = {
    PROFILING_CONFIG_KEY: Field(
        Permissive(
            {
                "enabled": Field(
                    Bool,
                    is_required=False,
                    default_value=False,
                    description="Wrap step commands with resource profiling.",
                ),
                "sample_interval_seconds": Field(
                    Float,
                    is_required=False,
                    default_value=DEFAULT_PROFILING_SAMPLE_INTERVAL_SECONDS,
                    description="Seconds between CPU/memory samples.",
                ),
            }
        ),
        is_required=False,
        default_value={},
        description="Resource profiling settings for each executed step.",
    )
}


@dataclass
class ProfilingConfig:
    enabled: bool = False
    sample_interval_seconds: float = DEFAULT_PROFILING_SAMPLE_INTERVAL_SECONDS

    @classmethod
    def from_config(cls, config: dict | None) -> ProfilingConfig:
        raw = (config or {}).get(PROFILING_CONFIG_KEY) or {}
        if not isinstance(raw, dict):
            raw = {}
        sample_interval_seconds = float(
            raw.get(
                "sample_interval_seconds",
                DEFAULT_PROFILING_SAMPLE_INTERVAL_SECONDS,
            )
        )
        return cls(
            enabled=bool(raw.get("enabled", False)),
            sample_interval_seconds=max(sample_interval_seconds, 0.1),
        )


def wrap_command_for_profiling(
    command: list[str], profiling: ProfilingConfig
) -> list[str]:
    if not profiling.enabled:
        return command
    return [
        "python",
        "-m",
        "cfa_dagster.execution.profile_step",
        "--sample-interval-seconds",
        str(profiling.sample_interval_seconds),
        "--",
        *command,
    ]


@dataclass
class ResourceSample:
    elapsed_seconds: float
    cpu_usage_seconds: float | None
    memory_bytes: int | None
    memory_peak_bytes: int | None


class ResourceSampler:
    source = "unavailable"

    def sample(self, elapsed_seconds: float) -> ResourceSample:
        return ResourceSample(elapsed_seconds, None, None, None)


class PsutilProcessTreeSampler(ResourceSampler):
    source = PROFILER_SOURCE_PSUTIL

    def __init__(self, pid: int):
        self._process = psutil.Process(pid)
        self._memory_peak_bytes = 0

    @classmethod
    def create(cls, pid: int | None) -> PsutilProcessTreeSampler | None:
        if pid is None:
            return None
        try:
            return cls(pid)
        except psutil.Error:
            log.debug("Unable to create psutil sampler for pid %s", pid, exc_info=True)
            return None

    def _processes(self) -> list[psutil.Process]:
        try:
            return [self._process, *self._process.children(recursive=True)]
        except psutil.Error:
            log.debug("Unable to read psutil process tree", exc_info=True)
            return []

    def sample(self, elapsed_seconds: float) -> ResourceSample:
        cpu_usage_seconds = 0.0
        memory_bytes = 0
        saw_cpu = False
        saw_memory = False
        for process in self._processes():
            try:
                cpu_times = process.cpu_times()
                cpu_usage_seconds += cpu_times.user + cpu_times.system
                saw_cpu = True
            except psutil.Error:
                log.debug("Unable to read psutil cpu times", exc_info=True)

            try:
                memory_bytes += process.memory_info().rss
                saw_memory = True
            except psutil.Error:
                log.debug("Unable to read psutil memory info", exc_info=True)

        if saw_memory:
            self._memory_peak_bytes = max(self._memory_peak_bytes, memory_bytes)

        return ResourceSample(
            elapsed_seconds=elapsed_seconds,
            cpu_usage_seconds=cpu_usage_seconds if saw_cpu else None,
            memory_bytes=memory_bytes if saw_memory else None,
            memory_peak_bytes=self._memory_peak_bytes if saw_memory else None,
        )


def _get_sampler(pid: int | None = None) -> ResourceSampler:
    return PsutilProcessTreeSampler.create(pid) or ResourceSampler()


def _round_float(value: float) -> float:
    return round(value, 2)


def _status_reason(cpu_status: str, memory_status: str) -> str | None:
    reasons = []
    if cpu_status != "ok":
        reasons.append(f"cpu: {cpu_status}")
    if memory_status != "ok":
        reasons.append(f"memory: {memory_status}")
    return "; ".join(reasons) if reasons else None


def _summarize_samples(
    samples: list[ResourceSample], source: str, duration_seconds: float
) -> dict[str, int | float | str]:
    memory_values = [s.memory_bytes for s in samples if s.memory_bytes is not None]
    memory_peak_values = [
        s.memory_peak_bytes for s in samples if s.memory_peak_bytes is not None
    ]
    cpu_samples = [s for s in samples if s.cpu_usage_seconds is not None]
    memory_status = "ok" if memory_values else "unavailable"
    cpu_status = "ok" if len(cpu_samples) >= 2 else "insufficient_samples"
    if not cpu_samples:
        cpu_status = "unavailable"

    summary: dict[str, int | float | str] = {
        "duration_seconds": _round_float(duration_seconds),
        "sample_count": len(samples),
        "profiler_source": source,
        "status": "ok" if samples else "failed",
    }

    if memory_values:
        memory_average_bytes = sum(memory_values) / len(memory_values)
        memory_peak_bytes = max(memory_peak_values or memory_values)
        summary.update(
            {
                "memory_average_gib": _round_float(
                    memory_average_bytes / 1024**3
                ),
                "memory_peak_gib": _round_float(memory_peak_bytes / 1024**3),
            }
        )

    if len(cpu_samples) >= 2:
        first = cpu_samples[0]
        last = cpu_samples[-1]
        assert first.cpu_usage_seconds is not None
        assert last.cpu_usage_seconds is not None
        cpu_usage_seconds_total = max(
            0.0, last.cpu_usage_seconds - first.cpu_usage_seconds
        )
        cpu_max_cores = 0.0
        for previous, current in pairwise(cpu_samples):
            assert previous.cpu_usage_seconds is not None
            assert current.cpu_usage_seconds is not None
            elapsed_delta = current.elapsed_seconds - previous.elapsed_seconds
            if elapsed_delta <= 0:
                continue
            cpu_delta = current.cpu_usage_seconds - previous.cpu_usage_seconds
            cpu_max_cores = max(cpu_max_cores, cpu_delta / elapsed_delta)
        summary.update(
            {
                "cpu_usage_seconds_total": _round_float(
                    cpu_usage_seconds_total
                ),
                "cpu_average_cores": cpu_usage_seconds_total
                / duration_seconds
                if duration_seconds > 0
                else 0.0,
                "cpu_max_cores": _round_float(cpu_max_cores),
            }
        )
        summary["cpu_average_cores"] = _round_float(
            float(summary["cpu_average_cores"])
        )

    if source == "unavailable":
        summary["status"] = "failed"
        summary["status_reason"] = "profiler source unavailable"
    elif cpu_status != "ok" or memory_status != "ok":
        summary["status"] = "partial"
        reason = _status_reason(cpu_status, memory_status)
        if reason:
            summary["status_reason"] = reason

    return summary


def _add_requested_resources(summary: dict[str, int | float | str]) -> None:
    requested_cpu = os.getenv("CFA_DAGSTER_REQUESTED_CPU_CORES")
    requested_memory = os.getenv("CFA_DAGSTER_REQUESTED_MEMORY_GIB")
    if requested_cpu:
        try:
            cpu = float(requested_cpu)
            summary["requested_cpu_cores"] = _round_float(cpu)
            if cpu > 0 and "cpu_max_cores" in summary:
                summary["cpu_max_percent_of_request"] = (
                    _round_float(float(summary["cpu_max_cores"]) / cpu * 100)
                )
        except ValueError:
            pass
    if requested_memory:
        try:
            memory = float(requested_memory)
            summary["requested_memory_gib"] = _round_float(memory)
            if memory > 0 and "memory_peak_gib" in summary:
                summary["memory_peak_percent_of_request"] = (
                    _round_float(float(summary["memory_peak_gib"]) / memory * 100)
                )
        except ValueError:
            pass


def _get_compressed_execute_step_args(command: list[str]) -> str | None:
    if "--compressed-input-json" in command:
        index = command.index("--compressed-input-json")
        if index + 1 < len(command):
            return command[index + 1]
    return os.getenv("DAGSTER_COMPRESSED_EXECUTE_STEP_ARGS")


def _get_instance_from_execute_step_command(command: list[str]):
    compressed_args = _get_compressed_execute_step_args(command)
    if not compressed_args:
        return None

    try:
        from dagster import DagsterInstance
        from dagster._grpc.types import ExecuteStepArgs
        from dagster._serdes import deserialize_value

        serialized_args = zlib.decompress(
            base64.b64decode(compressed_args)
        ).decode()
        execute_step_args = deserialize_value(serialized_args, ExecuteStepArgs)
        return DagsterInstance.from_ref(execute_step_args.instance_ref)
    except Exception:
        log.debug(
            "Unable to create Dagster instance from execute-step args",
            exc_info=True,
        )
        return None


def _report_profile(
    summary: dict[str, int | float | str], command: list[str] | None = None
) -> None:
    run_id = os.getenv("DAGSTER_RUN_ID")
    step_key = os.getenv("DAGSTER_RUN_STEP_KEY")
    if not run_id or not step_key:
        log.warning(
            "Unable to report step resource profile because DAGSTER_RUN_ID or "
            "DAGSTER_RUN_STEP_KEY is unset."
        )
        return

    try:
        from dagster import DagsterInstance
        from dagster._core.events import EngineEventData

        instance = _get_instance_from_execute_step_command(
            command or []
        ) or DagsterInstance.get()
        dagster_run = instance.get_run_by_id(run_id)
        if dagster_run is None:
            log.warning(
                "Unable to report step resource profile because run %s was not found.",
                run_id,
            )
            return
        instance.report_engine_event(
            message="Step resource profile",
            dagster_run=dagster_run,
            step_key=step_key,
            engine_event_data=EngineEventData(metadata=summary),
        )
    except Exception:
        log.warning("Unable to report step resource profile", exc_info=True)


def run_profiled_command(
    command: list[str], sample_interval_seconds: float
) -> int:
    sample_interval_seconds = max(sample_interval_seconds, 0.1)
    start = time.monotonic()
    process = subprocess.Popen(command)
    sampler = _get_sampler(process.pid)
    samples = [sampler.sample(0.0)]

    try:
        while process.poll() is None:
            time.sleep(sample_interval_seconds)
            samples.append(sampler.sample(time.monotonic() - start))
    except KeyboardInterrupt:
        process.terminate()
        raise
    finally:
        return_code = process.wait()

    duration_seconds = time.monotonic() - start
    samples.append(sampler.sample(duration_seconds))
    summary = _summarize_samples(samples, sampler.source, duration_seconds)
    summary["sample_interval_seconds"] = _round_float(sample_interval_seconds)
    _add_requested_resources(summary)
    _report_profile(summary, command)
    return return_code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sample-interval-seconds",
        type=float,
        default=DEFAULT_PROFILING_SAMPLE_INTERVAL_SECONDS,
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        parser.error("missing command to profile")
    return run_profiled_command(command, args.sample_interval_seconds)
