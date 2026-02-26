import hashlib
import logging
from typing import Optional

from dagster import (
    DagsterInstance,
    DagsterRun,
    QueuedRunCoordinator,
    SubmitRunContext,
)

log = logging.getLogger(__name__)

TARGET_TAG_KEY = "cfa_dagster/target"
MAX_TAG_LENGTH = 255


class CFAQueuedRunCoordinator(QueuedRunCoordinator):
    def _get_target_tag(
        self,
        run: DagsterRun,
        instance: DagsterInstance,
    ) -> Optional[dict]:
        """
        Returns a 'cfa_dagster/target' tag identifying the steps (ops/assets)
        and partitions for the run. Used with tag_concurrency_limits to prevent
        IO manager collisions during parallel runs.
        """
        log.debug(f"run: '{run}'")

        # -----------------------------
        # Determine target steps (ops/assets)
        # -----------------------------
        # Use step_keys_to_execute for all jobs
        if run.asset_selection:
            # Backfills and asset runs
            target_steps = sorted(
                "/".join(a.path) for a in run.asset_selection
            )
        elif run.step_keys_to_execute:
            # Regular ops/subset runs
            target_steps = sorted(run.step_keys_to_execute)

        # -----------------------------
        # Collect partitions (single + multi)
        # -----------------------------
        partitions = []
        partition_key = run.tags.get("dagster/partition")
        if partition_key:
            if "|" in partition_key:
                partitions = sorted(partition_key.split("|"))
            else:
                partitions = [partition_key]

        # -----------------------------
        # Combine deterministically
        # -----------------------------
        components = []

        if target_steps:
            components.append(",".join(target_steps))

        if partitions:
            components.append(",".join(partitions))

        if not components:
            return None

        full_value = "|".join(components)

        # -----------------------------
        # Enforce max length with hash fallback
        # -----------------------------
        if len(full_value) > MAX_TAG_LENGTH:
            digest = hashlib.sha1(full_value.encode("utf-8")).hexdigest()
            prefix_length = MAX_TAG_LENGTH - len(digest) - 3
            truncated = full_value[:prefix_length]
            tag_value = f"{truncated}...{digest}"
        else:
            tag_value = full_value

        return {TARGET_TAG_KEY: tag_value} if tag_value else None

    def submit_run(self, context: SubmitRunContext):
        run = context.dagster_run
        instance = self._instance

        target_tag = self._get_target_tag(run, instance)

        if target_tag:
            instance.add_run_tags(
                run.run_id,
                target_tag,
            )

        # Delegate to default behavior
        return super().submit_run(context)
