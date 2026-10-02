import base64
import sys
import zlib
from collections import namedtuple

from cfa_dagster.profiling import (
    PROFILER_SOURCE_PSUTIL,
    ProfilingConfig,
    PsutilProcessTreeSampler,
    ResourceSample,
    ResourceSampler,
    _add_requested_resources,
    _get_compressed_execute_step_args,
    _get_instance_from_execute_step_command,
    _get_sampler,
    _summarize_samples,
    run_profiled_command,
    wrap_command_for_profiling,
)


def test_wrap_command_for_profiling_disabled():
    command = ["dagster", "api", "execute_step"]

    assert wrap_command_for_profiling(command, ProfilingConfig()) == command


def test_wrap_command_for_profiling_enabled():
    command = ["dagster", "api", "execute_step"]

    wrapped = wrap_command_for_profiling(
        command,
        ProfilingConfig(enabled=True, sample_interval_seconds=2.5),
    )

    assert wrapped == [
        "python",
        "-m",
        "cfa_dagster.execution.profile_step",
        "--sample-interval-seconds",
        "2.5",
        "--",
        *command,
    ]


def test_summarize_samples_calculates_cpu_and_memory():
    gib = 1024**3
    summary = _summarize_samples(
        [
            ResourceSample(0.0, 10.0, gib, int(1.5 * gib)),
            ResourceSample(1.0, 11.5, 2 * gib, int(2.5 * gib)),
            ResourceSample(2.0, 13.0, 3 * gib, int(3.5 * gib)),
        ],
        "cgroup_v2",
        2.123,
    )

    assert summary["status"] == "ok"
    assert summary["duration_seconds"] == 2.12
    assert summary["sample_count"] == 3
    assert summary["cpu_usage_seconds_total"] == 3.0
    assert summary["cpu_average_cores"] == 1.41
    assert summary["cpu_max_cores"] == 1.5
    assert summary["memory_average_gib"] == 2.0
    assert summary["memory_peak_gib"] == 3.5
    assert "memory_average_bytes" not in summary
    assert "memory_peak_bytes" not in summary
    assert "cpu_status" not in summary
    assert "memory_status" not in summary
    assert "profiler_status" not in summary


def test_summarize_samples_calculates_optional_metrics():
    gib = 1024**3
    summary = _summarize_samples(
        [
            ResourceSample(
                0.0,
                10.0,
                gib,
                gib,
                thread_count=2,
                disk_read_bytes=gib,
                disk_write_bytes=2 * gib,
                network_bytes_sent=3 * gib,
                network_bytes_received=4 * gib,
                system_swap_used_bytes=gib,
            ),
            ResourceSample(
                1.0,
                11.0,
                gib,
                gib,
                thread_count=4,
                disk_read_bytes=2 * gib,
                disk_write_bytes=4 * gib,
                network_bytes_sent=6 * gib,
                network_bytes_received=8 * gib,
                system_swap_used_bytes=2 * gib,
            ),
        ],
        "psutil",
        1.0,
    )

    assert summary["status"] == "ok"
    assert summary["thread_count_average"] == 3.0
    assert summary["thread_count_max"] == 4
    assert summary["disk_read_gib_total"] == 1.0
    assert summary["disk_write_gib_total"] == 2.0
    assert summary["system_network_sent_gib_total"] == 3.0
    assert summary["system_network_received_gib_total"] == 4.0
    assert summary["system_swap_used_average_gib"] == 1.5
    assert summary["system_swap_used_peak_gib"] == 2.0
    assert "disk_read_bytes_total" not in summary
    assert "disk_write_bytes_total" not in summary
    assert "system_network_bytes_sent_total" not in summary
    assert "system_network_bytes_received_total" not in summary


def test_summarize_samples_reports_zero_cpu_delta():
    summary = _summarize_samples(
        [
            ResourceSample(0.0, 10.0, 100, 100),
            ResourceSample(1.0, 10.0, 100, 100),
        ],
        "psutil",
        1.0,
    )

    assert summary["status"] == "ok"
    assert summary["cpu_usage_seconds_total"] == 0.0
    assert summary["cpu_average_cores"] == 0.0
    assert summary["cpu_max_cores"] == 0.0
    assert "status_reason" not in summary


def test_summarize_samples_sums_positive_cpu_deltas():
    summary = _summarize_samples(
        [
            ResourceSample(0.0, 1.0, 100, 100),
            ResourceSample(1.0, 5.0, 100, 100),
            ResourceSample(2.0, 3.0, 100, 100),
        ],
        "psutil",
        2.0,
    )

    assert summary["status"] == "ok"
    assert summary["cpu_usage_seconds_total"] == 4.0
    assert summary["cpu_average_cores"] == 2.0
    assert summary["cpu_max_cores"] == 4.0


def test_summarize_samples_reports_resource_statuses_when_missing():
    summary = _summarize_samples(
        [ResourceSample(0.0, 10.0, None, None)],
        "psutil",
        1.0,
    )

    assert summary["status"] == "partial"
    assert summary["status_reason"] == "cpu: insufficient_samples; memory: unavailable"
    assert "cpu_usage_seconds_total" not in summary
    assert "memory_peak_gib" not in summary


def test_add_requested_resources_rounds_values(monkeypatch):
    monkeypatch.setenv("CFA_DAGSTER_REQUESTED_CPU_CORES", "1.3333")
    monkeypatch.setenv("CFA_DAGSTER_REQUESTED_MEMORY_GIB", "4.4444")
    summary = {"cpu_max_cores": 0.6666, "memory_peak_gib": 2.2222}

    _add_requested_resources(summary)

    assert summary["requested_cpu_cores"] == 1.33
    assert summary["cpu_max_percent_of_request"] == 50.0
    assert summary["requested_memory_gib"] == 4.44
    assert summary["memory_peak_percent_of_request"] == 50.0


def test_run_profiled_command_preserves_exit_code_and_reports(monkeypatch):
    reported = []

    class FakeSampler(ResourceSampler):
        source = "fake"

        def sample(self, elapsed_seconds):
            return ResourceSample(elapsed_seconds, elapsed_seconds, 100, 200)

    monkeypatch.setattr(
        "cfa_dagster.profiling._get_sampler", lambda pid=None: FakeSampler()
    )
    monkeypatch.setattr(
        "cfa_dagster.profiling._report_profile",
        lambda summary, command=None: reported.append((summary, command)),
    )

    return_code = run_profiled_command(
        [sys.executable, "-c", "import sys; sys.exit(7)"],
        sample_interval_seconds=0.01,
    )

    assert return_code == 7
    assert len(reported) == 1
    assert reported[0][0]["profiler_source"] == "fake"
    assert reported[0][0]["sample_interval_seconds"] == 0.1
    assert reported[0][1] == [sys.executable, "-c", "import sys; sys.exit(7)"]


def test_get_compressed_execute_step_args_from_command(monkeypatch):
    monkeypatch.delenv("DAGSTER_COMPRESSED_EXECUTE_STEP_ARGS", raising=False)

    assert (
        _get_compressed_execute_step_args(
            ["dagster", "api", "execute_step", "--compressed-input-json", "abc"]
        )
        == "abc"
    )


def test_get_compressed_execute_step_args_from_env(monkeypatch):
    monkeypatch.setenv("DAGSTER_COMPRESSED_EXECUTE_STEP_ARGS", "from-env")

    assert _get_compressed_execute_step_args(["dagster", "api", "execute_step"]) == "from-env"


def test_get_instance_from_execute_step_command(monkeypatch):
    compressed = base64.b64encode(zlib.compress(b"serialized")).decode()
    expected_instance = object()

    class FakeExecuteStepArgs:
        instance_ref = "instance-ref"

    monkeypatch.setattr(
        "dagster._serdes.deserialize_value",
        lambda value, klass: FakeExecuteStepArgs(),
    )
    monkeypatch.setattr(
        "dagster.DagsterInstance.from_ref",
        lambda instance_ref: expected_instance,
    )

    assert (
        _get_instance_from_execute_step_command(
            ["dagster", "api", "execute_step", "--compressed-input-json", compressed]
        )
        is expected_instance
    )


def test_psutil_sampler_sums_parent_and_children(monkeypatch):
    CpuTimes = namedtuple("CpuTimes", ["user", "system"])
    MemoryInfo = namedtuple("MemoryInfo", ["rss"])
    IoCounters = namedtuple("IoCounters", ["read_bytes", "write_bytes"])
    NetworkIoCounters = namedtuple("NetworkIoCounters", ["bytes_sent", "bytes_recv"])
    SwapMemory = namedtuple("SwapMemory", ["used"])

    class FakeProcess:
        def __init__(
            self,
            pid,
            children=None,
            cpu_user=0.0,
            cpu_system=0.0,
            rss=0,
            threads=1,
            read_bytes=0,
            write_bytes=0,
        ):
            self.pid = pid
            self._children = children or []
            self._cpu_user = cpu_user
            self._cpu_system = cpu_system
            self._rss = rss
            self._threads = threads
            self._read_bytes = read_bytes
            self._write_bytes = write_bytes

        def oneshot(self):
            class OneShot:
                def __enter__(self):
                    return None

                def __exit__(self, exc_type, exc_value, traceback):
                    return False

            return OneShot()

        def children(self, recursive=True):
            return self._children

        def cpu_times(self):
            return CpuTimes(self._cpu_user, self._cpu_system)

        def memory_info(self):
            return MemoryInfo(self._rss)

        def num_threads(self):
            return self._threads

        def io_counters(self):
            return IoCounters(self._read_bytes, self._write_bytes)

    child = FakeProcess(
        456,
        cpu_user=0.5,
        cpu_system=0.25,
        rss=200,
        threads=2,
        read_bytes=20,
        write_bytes=30,
    )
    parent = FakeProcess(
        123,
        [child],
        cpu_user=1.0,
        cpu_system=0.5,
        rss=100,
        threads=3,
        read_bytes=40,
        write_bytes=50,
    )
    monkeypatch.setattr("cfa_dagster.profiling.psutil.Process", lambda pid: parent)
    monkeypatch.setattr(
        "cfa_dagster.profiling.psutil.net_io_counters",
        lambda nowrap=True: NetworkIoCounters(100, 200),
    )
    monkeypatch.setattr(
        "cfa_dagster.profiling.psutil.swap_memory",
        lambda: SwapMemory(300),
    )

    sampler = PsutilProcessTreeSampler(123)
    sample = sampler.sample(0.0)

    assert sample.cpu_usage_seconds == 2.25
    assert sample.memory_bytes == 300
    assert sample.memory_peak_bytes == 300
    assert sample.thread_count == 5
    assert sample.disk_read_bytes == 60
    assert sample.disk_write_bytes == 80
    assert sample.network_bytes_sent == 100
    assert sample.network_bytes_received == 200
    assert sample.system_swap_used_bytes == 300


def test_get_sampler_uses_psutil(monkeypatch):
    class FakePsutilSampler(ResourceSampler):
        source = PROFILER_SOURCE_PSUTIL

    monkeypatch.setattr(
        "cfa_dagster.profiling.PsutilProcessTreeSampler.create",
        lambda pid: FakePsutilSampler(),
    )

    assert _get_sampler(123).source == PROFILER_SOURCE_PSUTIL


def test_get_sampler_falls_back_to_unavailable(monkeypatch):
    monkeypatch.setattr(
        "cfa_dagster.profiling.PsutilProcessTreeSampler.create",
        lambda pid: None,
    )

    assert _get_sampler(123).source == "unavailable"
