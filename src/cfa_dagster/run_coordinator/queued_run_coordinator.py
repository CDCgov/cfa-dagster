from dagster._core.run_coordinator import QueuedRunCoordinator
from dagster._core.run_coordinator.base import SubmitRunContext
from dagster._core.storage.dagster_run import DagsterRun
import os


class UserQueuedRunCoordinator(QueuedRunCoordinator):
    def submit_run(self, context: SubmitRunContext) -> DagsterRun:
        dagster_run = context.dagster_run
        user = os.environ["USER"]
        if user is None or dagster_run.tags.get("user") != user:
            context.logger.error(f"Environment variable 'USER' must be set and the run must be tagged with your 'USER'" +
                                 'e.g. tags={"user"="<your_username>"')
            return None
        return super().submit_run(context)
