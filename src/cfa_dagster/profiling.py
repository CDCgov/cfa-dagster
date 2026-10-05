from __future__ import annotations

import argparse
import base64
import contextlib
import logging
import os
import subprocess
import threading
import time
import zlib
from contextlib import nullcontext
from dataclasses import dataclass
from itertools import pairwise

import psutil
from dagster import Bool, Field, Float, Permissive

log = logging.getLogger(__name__)

DEFAULT_PROFILING_SAMPLE_INTERVAL_SECONDS = 1.0
PROFILING_CONFIG_KEY = "profiling"
PROFILER_SOURCE_ENV = "CFA_DAGSTER_PROFILER_SOURCE"
PROFILER_SOURCE_PSUTIL = "psutil"
PROFILER_ASSET_KEY_ENV = "CFA_DAGSTER_PROFILE_ASSET_KEY"
PROFILER_PARTITION_KEY_ENV = "CFA_DAGSTER_PROFILE_PARTITION_KEY"

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
    command: list[str],
    profiling: ProfilingConfig,
    *,
    track_step_status: bool = False,
) -> list[str]:
    if not profiling.enabled:
        return command
    wrapped = [
        "python",
        "-m",
        "cfa_dagster.profile_step",
        "--sample-interval-seconds",
        str(profiling.sample_interval_seconds),
    ]
    if track_step_status:
        wrapped.append("--track-step-status")
    return [*wrapped, "--", *command]


@dataclass
class ResourceSample:
    elapsed_seconds: float
    cpu_usage_seconds: float | None
    memory_bytes: int | None
    memory_peak_bytes: int | None
    thread_count: int | None = None
    disk_read_bytes: int | None = None
    disk_write_bytes: int | None = None
    network_bytes_sent: int | None = None
    network_bytes_received: int | None = None
    system_swap_used_bytes: int | None = None


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
            log.debug(
                "Unable to create psutil sampler for pid %s",
                pid,
                exc_info=True,
            )
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
        thread_count = 0
        disk_read_bytes = 0
        disk_write_bytes = 0
        saw_cpu = False
        saw_memory = False
        saw_threads = False
        saw_disk = False
        for process in self._processes():
            try:
                oneshot = getattr(process, "oneshot", nullcontext)
                with oneshot():
                    try:
                        cpu_times = process.cpu_times()
                        cpu_usage_seconds += cpu_times.user + cpu_times.system
                        saw_cpu = True
                    except psutil.Error:
                        log.debug(
                            "Unable to read psutil cpu times", exc_info=True
                        )

                    try:
                        memory_bytes += process.memory_info().rss
                        saw_memory = True
                    except psutil.Error:
                        log.debug(
                            "Unable to read psutil memory info", exc_info=True
                        )

                    try:
                        thread_count += process.num_threads()
                        saw_threads = True
                    except (AttributeError, psutil.Error):
                        log.debug(
                            "Unable to read psutil thread count", exc_info=True
                        )

                    try:
                        io_counters = process.io_counters()
                        disk_read_bytes += io_counters.read_bytes
                        disk_write_bytes += io_counters.write_bytes
                        saw_disk = True
                    except (AttributeError, psutil.Error):
                        log.debug(
                            "Unable to read psutil disk io", exc_info=True
                        )
            except psutil.Error:
                log.debug("Unable to sample psutil process", exc_info=True)

        if saw_memory:
            self._memory_peak_bytes = max(
                self._memory_peak_bytes, memory_bytes
            )

        try:
            net_io = psutil.net_io_counters(nowrap=True)
        except psutil.Error:
            log.debug("Unable to read psutil network io", exc_info=True)
            net_io = None

        try:
            swap = psutil.swap_memory()
        except psutil.Error:
            log.debug("Unable to read psutil swap memory", exc_info=True)
            swap = None

        return ResourceSample(
            elapsed_seconds=elapsed_seconds,
            cpu_usage_seconds=cpu_usage_seconds if saw_cpu else None,
            memory_bytes=memory_bytes if saw_memory else None,
            memory_peak_bytes=self._memory_peak_bytes if saw_memory else None,
            thread_count=thread_count if saw_threads else None,
            disk_read_bytes=disk_read_bytes if saw_disk else None,
            disk_write_bytes=disk_write_bytes if saw_disk else None,
            network_bytes_sent=net_io.bytes_sent if net_io else None,
            network_bytes_received=net_io.bytes_recv if net_io else None,
            system_swap_used_bytes=swap.used if swap else None,
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
    memory_values = [
        s.memory_bytes for s in samples if s.memory_bytes is not None
    ]
    memory_peak_values = [
        s.memory_peak_bytes for s in samples if s.memory_peak_bytes is not None
    ]
    cpu_samples = [s for s in samples if s.cpu_usage_seconds is not None]
    thread_values = [
        s.thread_count for s in samples if s.thread_count is not None
    ]
    disk_samples = [
        s
        for s in samples
        if s.disk_read_bytes is not None and s.disk_write_bytes is not None
    ]
    network_samples = [
        s
        for s in samples
        if s.network_bytes_sent is not None
        and s.network_bytes_received is not None
    ]
    swap_values = [
        s.system_swap_used_bytes
        for s in samples
        if s.system_swap_used_bytes is not None
    ]
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
        cpu_usage_seconds_total = 0.0
        cpu_max_cores = 0.0
        for previous, current in pairwise(cpu_samples):
            assert previous.cpu_usage_seconds is not None
            assert current.cpu_usage_seconds is not None
            elapsed_delta = current.elapsed_seconds - previous.elapsed_seconds
            if elapsed_delta <= 0:
                continue
            cpu_delta = max(
                0.0, current.cpu_usage_seconds - previous.cpu_usage_seconds
            )
            cpu_usage_seconds_total += cpu_delta
            cpu_max_cores = max(cpu_max_cores, cpu_delta / elapsed_delta)
        summary.update(
            {
                "cpu_usage_seconds_total": _round_float(
                    cpu_usage_seconds_total
                ),
                "cpu_average_cores": cpu_usage_seconds_total / duration_seconds
                if duration_seconds > 0
                else 0.0,
                "cpu_max_cores": _round_float(cpu_max_cores),
            }
        )
        summary["cpu_average_cores"] = _round_float(
            float(summary["cpu_average_cores"])
        )

    if thread_values:
        summary.update(
            {
                "thread_count_average": _round_float(
                    sum(thread_values) / len(thread_values)
                ),
                "thread_count_max": max(thread_values),
            }
        )

    if len(disk_samples) >= 2:
        first = disk_samples[0]
        last = disk_samples[-1]
        assert first.disk_read_bytes is not None
        assert first.disk_write_bytes is not None
        assert last.disk_read_bytes is not None
        assert last.disk_write_bytes is not None
        summary.update(
            {
                "disk_read_gib_total": _round_float(
                    max(0, last.disk_read_bytes - first.disk_read_bytes)
                    / 1024**3
                ),
                "disk_write_gib_total": _round_float(
                    max(0, last.disk_write_bytes - first.disk_write_bytes)
                    / 1024**3
                ),
            }
        )

    if len(network_samples) >= 2:
        first = network_samples[0]
        last = network_samples[-1]
        assert first.network_bytes_sent is not None
        assert first.network_bytes_received is not None
        assert last.network_bytes_sent is not None
        assert last.network_bytes_received is not None
        summary.update(
            {
                "system_network_sent_gib_total": _round_float(
                    max(0, last.network_bytes_sent - first.network_bytes_sent)
                    / 1024**3
                ),
                "system_network_received_gib_total": _round_float(
                    max(
                        0,
                        last.network_bytes_received
                        - first.network_bytes_received,
                    )
                    / 1024**3
                ),
            }
        )

    if swap_values:
        summary.update(
            {
                "system_swap_used_average_gib": _round_float(
                    sum(swap_values) / len(swap_values) / 1024**3
                ),
                "system_swap_used_peak_gib": _round_float(
                    max(swap_values) / 1024**3
                ),
            }
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
                summary["cpu_max_percent_of_request"] = _round_float(
                    float(summary["cpu_max_cores"]) / cpu * 100
                )
        except ValueError:
            pass
    if requested_memory:
        try:
            memory = float(requested_memory)
            summary["requested_memory_gib"] = _round_float(memory)
            if memory > 0 and "memory_peak_gib" in summary:
                summary["memory_peak_percent_of_request"] = _round_float(
                    float(summary["memory_peak_gib"]) / memory * 100
                )
        except ValueError:
            pass


def _get_compressed_execute_step_args(command: list[str]) -> str | None:
    if "--compressed-input-json" in command:
        index = command.index("--compressed-input-json")
        if index + 1 < len(command):
            return command[index + 1]
    return os.getenv("DAGSTER_COMPRESSED_EXECUTE_STEP_ARGS")


def _get_execute_step_args_json(command: list[str]) -> str | None:
    compressed_args = _get_compressed_execute_step_args(command)
    if compressed_args:
        return zlib.decompress(base64.b64decode(compressed_args)).decode()
    with contextlib.suppress(ValueError):
        index = command.index("execute_step")
        if index + 1 < len(command):
            return command[index + 1]
    return os.getenv("DAGSTER_EXECUTE_STEP_ARGS")


def _get_instance_from_execute_step_command(command: list[str]):
    serialized_args = _get_execute_step_args_json(command)
    if not serialized_args:
        return None

    try:
        from dagster import DagsterInstance
        from dagster._grpc.types import ExecuteStepArgs
        from dagster._serdes import deserialize_value

        execute_step_args = deserialize_value(serialized_args, ExecuteStepArgs)
        return DagsterInstance.from_ref(execute_step_args.instance_ref)
    except Exception:
        log.debug(
            "Unable to create Dagster instance from execute-step args",
            exc_info=True,
        )
        return None


def _execute_step_and_get_success(command: list[str]) -> bool | None:
    serialized_args = _get_execute_step_args_json(command)
    if not serialized_args:
        return None

    from dagster import _check as check
    from dagster._cli.api import (
        _execute_step_command_body,
        get_instance_for_cli,
    )
    from dagster._core.events import DagsterEventType
    from dagster._grpc.types import ExecuteStepArgs
    from dagster._serdes import deserialize_value
    from dagster._utils.interrupts import capture_interrupts

    execute_step_args = deserialize_value(serialized_args, ExecuteStepArgs)
    step_key = (
        execute_step_args.step_keys_to_execute[0]
        if execute_step_args.step_keys_to_execute
        and len(execute_step_args.step_keys_to_execute) == 1
        else None
    )
    succeeded = None
    with (
        capture_interrupts(),
        get_instance_for_cli(
            instance_ref=execute_step_args.instance_ref
        ) as instance,
    ):
        dagster_run = check.not_none(
            instance.get_run_by_id(execute_step_args.run_id),
            f"Run with id '{execute_step_args.run_id}' not found for step execution",
        )
        for event in _execute_step_command_body(
            execute_step_args,
            instance,
            dagster_run,
        ):
            if event.step_key != step_key:
                continue
            if event.event_type_value == DagsterEventType.STEP_SUCCESS.value:
                succeeded = True
            elif event.event_type_value == DagsterEventType.STEP_FAILURE.value:
                succeeded = False
    return succeeded


def get_profile_asset_observation_env(step_context) -> dict[str, str]:
    try:
        job_def = getattr(step_context, "job_def", None)
        if job_def is None:
            job_def = step_context.job.get_definition()
        asset_layer = job_def.asset_layer
        selected_keys = asset_layer.get_selected_entity_keys_for_node(
            step_context.node_handle
        )
        matches = []
        for step_output in step_context.step.step_outputs:
            asset_key = asset_layer.get_asset_key_for_node_output(
                step_context.node_handle,
                step_output.name,
            )
            if asset_key is None or asset_key not in selected_keys:
                continue
            matches.append((asset_key, step_output.name))
    except Exception:  # noqa: BLE001 - fail closed rather than blocking step launch.
        return {}

    if len(matches) != 1:
        return {}
    asset_key, output_name = matches[0]
    env = {PROFILER_ASSET_KEY_ENV: asset_key.to_user_string()}
    has_asset_partitions_for_output = getattr(
        step_context, "has_asset_partitions_for_output", None
    )
    if has_asset_partitions_for_output is None:
        partition_key = step_context.dagster_run.tags.get("dagster/partition")
        if partition_key:
            env[PROFILER_PARTITION_KEY_ENV] = partition_key
        return env
    if has_asset_partitions_for_output(output_name):
        with contextlib.suppress(Exception):
            partition_range = (
                step_context.asset_partition_key_range_for_output(output_name)
            )
            if partition_range.start != partition_range.end:
                return {}
            env[PROFILER_PARTITION_KEY_ENV] = partition_range.start
            return env
        return {}
    return env


def _get_profile_asset_observation_from_env():
    asset_key = os.getenv(PROFILER_ASSET_KEY_ENV)
    if not asset_key:
        return None
    try:
        from dagster import AssetKey

        return AssetKey.from_user_string(asset_key), os.getenv(
            PROFILER_PARTITION_KEY_ENV
        )
    except Exception:
        log.warning(
            "Unable to parse profiler asset observation env", exc_info=True
        )
        return None


def _report_asset_observations(
    instance,
    dagster_run,
    run_id: str,
    step_key: str,
    summary: dict[str, int | float | str],
) -> bool:
    try:
        from dagster import AssetObservation
        from dagster._core.events import (
            AssetObservationData,
            DagsterEvent,
            DagsterEventType,
        )

        observation_context = _get_profile_asset_observation_from_env()
        if observation_context is None:
            return False
        asset_key, partition_key = observation_context
        observation = AssetObservation(
            asset_key=asset_key,
            description="Step resource profile",
            metadata=summary,
            partition=partition_key,
            tags={"cfa_dagster/profiling": "true"},
        )
        instance.report_dagster_event(
            DagsterEvent(
                event_type_value=DagsterEventType.ASSET_OBSERVATION.value,
                job_name=dagster_run.job_name,
                step_key=step_key,
                event_specific_data=AssetObservationData(observation),
                message="Observed step resource profile.",
            ),
            run_id=run_id,
        )
        return True
    except Exception:
        log.warning(
            "Unable to report asset resource profile observations",
            exc_info=True,
        )
        return False


def _report_profile(
    summary: dict[str, int | float | str],
    command: list[str] | None = None,
    *,
    emit_asset_observations: bool = True,
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

        instance = (
            _get_instance_from_execute_step_command(command or [])
            or DagsterInstance.get()
        )
        dagster_run = instance.get_run_by_id(run_id)
        if dagster_run is None:
            log.warning(
                "Unable to report step resource profile because run %s was not found.",
                run_id,
            )
            return
        if emit_asset_observations:
            reported_asset_observation = _report_asset_observations(
                instance,
                dagster_run,
                run_id,
                step_key,
                summary,
            )
            if reported_asset_observation:
                return
        instance.report_engine_event(
            message="Step resource profile",
            dagster_run=dagster_run,
            step_key=step_key,
            engine_event_data=EngineEventData(metadata=summary),
        )
    except Exception:
        log.warning("Unable to report step resource profile", exc_info=True)


def _sample_until_stopped(
    sampler: ResourceSampler,
    samples: list[ResourceSample],
    start: float,
    sample_interval_seconds: float,
    stop: threading.Event,
) -> None:
    while not stop.wait(sample_interval_seconds):
        samples.append(sampler.sample(time.monotonic() - start))


def run_profiled_command(
    command: list[str],
    sample_interval_seconds: float,
    *,
    track_step_status: bool = False,
) -> int:
    sample_interval_seconds = max(sample_interval_seconds, 0.1)
    start = time.monotonic()
    if track_step_status:
        sampler = _get_sampler(os.getpid())
        samples = [sampler.sample(0.0)]
        stop_sampling = threading.Event()
        sampler_thread = threading.Thread(
            target=_sample_until_stopped,
            args=(
                sampler,
                samples,
                start,
                sample_interval_seconds,
                stop_sampling,
            ),
            daemon=True,
        )
        sampler_thread.start()
        try:
            succeeded = _execute_step_and_get_success(command)
            return_code = 0
        except KeyboardInterrupt:
            raise
        except Exception:
            log.warning(
                "Unable to execute tracked step command", exc_info=True
            )
            succeeded = False
            return_code = 1
        finally:
            stop_sampling.set()
            sampler_thread.join(timeout=30)

        duration_seconds = time.monotonic() - start
        samples.append(sampler.sample(duration_seconds))
        summary = _summarize_samples(samples, sampler.source, duration_seconds)
        summary["sample_interval_seconds"] = _round_float(
            sample_interval_seconds
        )
        _add_requested_resources(summary)
        _report_profile(
            summary,
            command,
            emit_asset_observations=succeeded is True,
        )
        return return_code

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
    _report_profile(
        summary,
        command,
        emit_asset_observations=return_code == 0,
    )
    return return_code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sample-interval-seconds",
        type=float,
        default=DEFAULT_PROFILING_SAMPLE_INTERVAL_SECONDS,
    )
    parser.add_argument("--track-step-status", action="store_true")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        parser.error("missing command to profile")
    return run_profiled_command(
        command,
        args.sample_interval_seconds,
        track_step_status=args.track_step_status,
    )


if __name__ == "__main__":
    raise SystemExit(main())
