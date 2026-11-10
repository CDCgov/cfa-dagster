import logging
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import dagster as dg
import pytest
from dagster._core.errors import DagsterExecutionInterruptedError
from dagster._core.events import DagsterEventType
from dagster._core.execution.retries import RetryMode
from dagster._core.execution.stats import StepEventStatus
from dagster._core.storage.dagster_run import DagsterRunStatus
from dagster._grpc.types import ExecuteStepArgs

from cfa_dagster.azure_batch.execute_step import execute_step_events


@dg.op
def some_step():
    return "done"


@dg.job
def worker_guard_job():
    some_step()


def _step_args(instance, run):
    return ExecuteStepArgs(
        job_origin=dg.reconstructable(worker_guard_job).get_python_origin(),
        run_id=run.run_id,
        step_keys_to_execute=["some_step"],
        instance_ref=instance.get_ref(),
        retry_mode=RetryMode.DISABLED,
    )


@pytest.fixture
def failed_run():
    with dg.DagsterInstance.local_temp() as instance:
        run = instance.create_run_for_job(
            worker_guard_job, status=DagsterRunStatus.FAILURE
        )
        yield instance, run


def test_late_worker_records_one_terminal_failure(failed_run):
    instance, run = failed_run
    args = _step_args(instance, run)
    emitted = []

    with patch(
        "cfa_dagster.azure_batch.execute_step._execute_step_command_body"
    ) as execute:
        with pytest.raises(
            DagsterExecutionInterruptedError, match="already failed"
        ):
            emitted.extend(execute_step_events(args, instance, run))
        with pytest.raises(DagsterExecutionInterruptedError):
            list(execute_step_events(args, instance, run))
        execute.assert_not_called()

    assert len(emitted) == 1
    assert emitted[0].is_step_failure
    assert emitted[0].step_key == "some_step"
    assert emitted[0].step_failure_data.error_source.value == "INTERRUPT"
    assert "already failed" in emitted[0].step_failure_data.error.message
    metadata = emitted[0].step_failure_data.user_failure_data.metadata
    assert metadata["run_status"].value == "FAILURE"
    assert metadata["reason"].value == "run_failed_before_step_start"
    records = instance.all_logs(
        run.run_id, of_type=DagsterEventType.STEP_FAILURE
    )
    assert len(records) == 1
    assert records[0].level == logging.ERROR
    assert (
        instance.get_run_step_stats(run.run_id)[0].status
        == StepEventStatus.FAILURE
    )


@pytest.mark.parametrize(
    "status", [StepEventStatus.SUCCESS, StepEventStatus.SKIPPED]
)
def test_late_worker_preserves_completed_step_status(status):
    instance = Mock()
    instance.get_run_step_stats.return_value = [
        SimpleNamespace(step_key="some_step", status=status)
    ]
    args = SimpleNamespace(run_id="run-id", step_keys_to_execute=["some_step"])
    run = Mock(status=DagsterRunStatus.FAILURE, job_name="test-job")

    with pytest.raises(DagsterExecutionInterruptedError):
        list(execute_step_events(args, instance, run))

    instance.report_dagster_event.assert_not_called()


@pytest.mark.parametrize(
    "status",
    [
        DagsterRunStatus.STARTED,
        DagsterRunStatus.SUCCESS,
        DagsterRunStatus.CANCELED,
        DagsterRunStatus.CANCELING,
    ],
)
def test_other_run_states_keep_dagster_behavior(status):
    instance = Mock()
    args = Mock()
    run = Mock(status=status)
    event = Mock()
    with patch(
        "cfa_dagster.azure_batch.execute_step._execute_step_command_body",
        return_value=iter([event]),
    ) as execute:
        assert list(execute_step_events(args, instance, run)) == [event]
        execute.assert_called_once_with(args, instance, run)
    instance.get_run_step_stats.assert_not_called()
    instance.report_dagster_event.assert_not_called()


@pytest.mark.parametrize("mode", ["direct", "profiled", "tracked"])
def test_late_worker_process_exits_nonzero_and_marks_step_failed(
    failed_run, mode
):
    instance, run = failed_run
    args = _step_args(instance, run)
    env = dict(os.environ)
    env.pop("DAGSTER_EXECUTE_STEP_ARGS", None)
    env.update(
        {
            setting["name"]: setting["value"]
            for setting in args.get_command_env()
        }
    )
    env.update(
        DAGSTER_RUN_ID=run.run_id,
        DAGSTER_RUN_JOB_NAME=run.job_name,
        DAGSTER_RUN_STEP_KEY="some_step",
    )
    command = [sys.executable, "-m", "cfa_dagster.azure_batch.execute_step"]
    if mode != "direct":
        command = [
            sys.executable,
            "-m",
            "cfa_dagster.profile_step",
            "--sample-interval-seconds",
            "0.1",
            *(["--track-step-status"] if mode == "tracked" else []),
            "--",
            *command,
        ]

    result = subprocess.run(
        command,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert result.returncode == 1, result.stderr
    assert "already failed" in result.stderr
    records = instance.all_logs(
        run.run_id, of_type=DagsterEventType.STEP_FAILURE
    )
    assert len(records) == 1
    assert (
        instance.get_run_step_stats(run.run_id)[0].status
        == StepEventStatus.FAILURE
    )


def test_worker_still_executes_an_active_run():
    with dg.DagsterInstance.local_temp() as instance:
        origin = dg.reconstructable(worker_guard_job).get_python_origin()
        run = instance.create_run_for_job(
            worker_guard_job,
            status=DagsterRunStatus.STARTED,
            job_code_origin=origin,
        )
        args = _step_args(instance, run)
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(
            [str(Path(__file__).parent), env.get("PYTHONPATH", "")]
        )
        env.pop("DAGSTER_EXECUTE_STEP_ARGS", None)
        env.update(
            {
                setting["name"]: setting["value"]
                for setting in args.get_command_env()
            }
        )

        result = subprocess.run(
            [sys.executable, "-m", "cfa_dagster.azure_batch.execute_step"],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

        assert result.returncode == 0, result.stderr
        assert (
            instance.get_run_step_stats(run.run_id)[0].status
            == StepEventStatus.SUCCESS
        )
