"""Batch worker entry point that records failures for late-starting steps."""

import base64
import logging
import os
import sys
import zlib
from collections.abc import Iterator

import dagster._check as check
from dagster import DagsterInstance, DagsterRun
from dagster._cli.api import (
    _execute_step_command_body,
    get_instance_for_cli,
)
from dagster._core.errors import DagsterExecutionInterruptedError
from dagster._core.events import DagsterEvent, DagsterEventType
from dagster._core.execution.plan.objects import (
    ErrorSource,
    StepFailureData,
    UserFailureData,
)
from dagster._core.execution.stats import StepEventStatus
from dagster._core.storage.dagster_run import DagsterRunStatus
from dagster._grpc.types import ExecuteStepArgs
from dagster._serdes import deserialize_value, serialize_value
from dagster._utils.error import serializable_error_info_from_exc_info
from dagster._utils.interrupts import capture_interrupts


def execute_step_events(
    args: ExecuteStepArgs, instance: DagsterInstance, dagster_run: DagsterRun
) -> Iterator[DagsterEvent]:
    if dagster_run.status != DagsterRunStatus.FAILURE:
        yield from _execute_step_command_body(args, instance, dagster_run)
        return

    step_keys = check.not_none(args.step_keys_to_execute)
    step_statuses = {
        stat.step_key: stat.status
        for stat in instance.get_run_step_stats(args.run_id, step_keys)
    }
    terminal_statuses = {
        StepEventStatus.SUCCESS,
        StepEventStatus.FAILURE,
        StepEventStatus.SKIPPED,
    }
    try:
        raise DagsterExecutionInterruptedError(
            f"Batch worker cannot execute steps because run {args.run_id} "
            "has already failed."
        )
    except DagsterExecutionInterruptedError:
        error_info = serializable_error_info_from_exc_info(sys.exc_info())
        for step_key in step_keys:
            if step_statuses.get(step_key) in terminal_statuses:
                continue
            event = DagsterEvent(
                event_type_value=DagsterEventType.STEP_FAILURE.value,
                job_name=dagster_run.job_name,
                step_key=step_key,
                message=(
                    f"Step {step_key} was not executed because run "
                    f"{args.run_id} had already failed."
                ),
                event_specific_data=StepFailureData(
                    error=error_info,
                    error_source=ErrorSource.INTERRUPT,
                    user_failure_data=UserFailureData(
                        label="Azure Batch step aborted",
                        metadata={
                            "run_id": args.run_id,
                            "run_status": dagster_run.status.value,
                            "reason": "run_failed_before_step_start",
                        },
                    ),
                ),
                pid=os.getpid(),
            )
            instance.report_dagster_event(
                event, run_id=args.run_id, log_level=logging.ERROR
            )
            yield event
        raise


def main() -> int:
    input_json = os.getenv("DAGSTER_EXECUTE_STEP_ARGS")
    compressed_json = os.getenv("DAGSTER_COMPRESSED_EXECUTE_STEP_ARGS")
    check.invariant(
        bool(input_json) != bool(compressed_json),
        "Provide exactly one of DAGSTER_EXECUTE_STEP_ARGS or "
        "DAGSTER_COMPRESSED_EXECUTE_STEP_ARGS.",
    )
    if compressed_json:
        input_json = zlib.decompress(
            base64.b64decode(compressed_json)
        ).decode()
    args = deserialize_value(check.not_none(input_json), ExecuteStepArgs)
    with (
        capture_interrupts(),
        get_instance_for_cli(instance_ref=args.instance_ref) as instance,
    ):
        dagster_run = check.not_none(
            instance.get_run_by_id(args.run_id),
            f"Run with id {args.run_id} not found for step execution",
        )
        events = list(execute_step_events(args, instance, dagster_run))
        if args.print_serialized_events:
            for event in events:
                print(serialize_value(event))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
