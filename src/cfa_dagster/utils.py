from dagster import (
    AssetsDefinition,
    JobDefinition,
    ScheduleDefinition,
    SensorDefinition
)


def collect_definitions(namespace):
    assets = []
    jobs = []
    schedules = []
    sensors = []

    for obj in list(namespace.values()):
        if isinstance(obj, AssetsDefinition):
            assets.append(obj)
        elif isinstance(obj, JobDefinition):
            jobs.append(obj)
        elif isinstance(obj, ScheduleDefinition):
            schedules.append(obj)
        elif isinstance(obj, SensorDefinition):
            sensors.append(obj)

    return {
        "assets": assets,
        "jobs": jobs,
        "schedules": schedules,
        "sensors": sensors,
    }

