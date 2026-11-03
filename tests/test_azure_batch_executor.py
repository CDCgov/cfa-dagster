import shlex
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from azure.core.exceptions import HttpResponseError
from dagster_docker.container_context import DockerContainerContext

from cfa_dagster.azure_batch.executor import AzureBatchStepHandler
from cfa_dagster.profiling import ProfilingConfig, wrap_command_for_profiling


@pytest.mark.parametrize("profiling_enabled", [False, True])
@pytest.mark.parametrize("submission_fails", [False, True])
def test_launch_metadata_is_emitted_before_submission(
    profiling_enabled, submission_fails
):
    handler = AzureBatchStepHandler.__new__(AzureBatchStepHandler)
    handler._batch_client = Mock()
    handler._subscription_id = "subscription"
    handler._pool_id = "pool"
    handler._profiling = ProfilingConfig(enabled=profiling_enabled)
    handler._get_docker_container_context = Mock(
        return_value=DockerContainerContext(
            container_kwargs={"working_dir": "/app"}
        )
    )
    handler._get_image = Mock(return_value="example/image:latest")
    handler._get_step_key = Mock(return_value="step")
    handler._get_job_id = Mock(return_value="job")
    handler._get_task_id = Mock(return_value="task")
    handler._get_or_create_job = Mock()
    command = ["dagster", "api", "execute_step", "--step-key", "caf\u00e9"]
    execute_step_args = Mock()
    execute_step_args.get_command_args.return_value = command
    execute_step_args.get_command_env.return_value = [
        {"name": "DAGSTER_COMPRESSED_EXECUTE_STEP_ARGS", "value": "secret"}
    ]
    context = SimpleNamespace(
        execute_step_args=execute_step_args,
        dagster_run=SimpleNamespace(job_name="job", run_id="run"),
        get_step_context=Mock(),
    )
    error = HttpResponseError(message="submission failed")
    if submission_fails:
        handler._batch_client.create_task.side_effect = error

    with (
        patch(
            "cfa_dagster.azure_batch.executor."
            "get_profile_asset_observation_env",
            return_value={},
        ),
        patch(
            "cfa_dagster.azure_batch.executor.DagsterEvent.engine_event"
        ) as engine_event,
        patch(
            "cfa_dagster.azure_batch.executor."
            "DagsterEvent.step_worker_starting"
        ) as worker_starting,
    ):
        events = handler.launch_step(context)
        assert next(events) is engine_event.return_value
        handler._batch_client.create_task.assert_not_called()
        metadata = engine_event.call_args.kwargs[
            "event_specific_data"
        ].metadata
        command_line = shlex.join(
            wrap_command_for_profiling(command, handler._profiling)
        )
        assert metadata["base_command_preview"].value == shlex.join(command)
        assert metadata["commandLine_characters"].value == len(command_line)
        assert metadata["commandLine_utf8_bytes"].value == len(
            command_line.encode("utf-8")
        )
        assert metadata["commandLine_utf8_bytes"].value > metadata[
            "commandLine_characters"
        ].value
        assert "secret" not in repr(metadata)

        if submission_fails:
            with pytest.raises(HttpResponseError) as raised:
                next(events)
            assert raised.value is error
            worker_starting.assert_not_called()
        else:
            assert next(events) is worker_starting.return_value
            starting_metadata = worker_starting.call_args.kwargs["metadata"]
            for key in (
                "base_command_preview",
                "commandLine_characters",
                "commandLine_utf8_bytes",
            ):
                assert starting_metadata[key] == metadata[key]
            assert starting_metadata["Azure Batch Job ID"] == "job"
            assert starting_metadata["Azure Batch Pool ID"] == "pool"
            with pytest.raises(StopIteration):
                next(events)

        task = handler._batch_client.create_task.call_args.kwargs["task"]
        assert task.command_line == command_line
        assert {
            setting.name: setting.value
            for setting in task.environment_settings
        }["DAGSTER_COMPRESSED_EXECUTE_STEP_ARGS"] == "secret"
        for call in execute_step_args.get_command_args.call_args_list:
            assert call.kwargs == {"skip_serialized_namedtuple": True}
