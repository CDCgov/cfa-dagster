# Concepts

The purpose of this page is to provide a high-level overview of the tools commonly used in CFA Dagster projects. This is not a comprehensive list of all Dagster capabilities. For additional information on Dagster concepts, click on [this link](https://docs.dagster.io/getting-started/concepts).

Dagster is an **asset-centric workflow orchestration tool** versus a task-centric workflow tool. An asset-centric workflow focuses on outputs and has upstream assets that are used as inputs to create their downstream [dependencies](https://docs.dagster.io/dagster-basics-tutorial/dependencies). A task-centric tool focuses on steps taken in a workflow. For example, if we were baking cookies, a task-centric workflow would include steps like gathering ingredients, combining ingredients, adding chocolate chips, baking in the oven, and eating the cookies. On the other hand, with the cookie example, an asset-centric workflow approach would look like the following: to create cookie dough, combine the wet ingredients with the dry ingredients; to create chocolate chip cookie dough, mix in the chocolate chips into the cookie dough; bake the chocolate chip cookie dough to eat freshly baked chocolate chip cookies. (Cookie example from [Dagster University Dagster Essentials course](https://courses.dagster.io/courses/dagster-essentials)) 

## Assets

A Dagster [asset](https://docs.dagster.io/dagster-basics-tutorial/assets) is a piece of data that your data pipeline creates, updates, or manages. Instead of focusing on what code runs, Dagster focuses on what data exists and how it depends on other data. Dagster treats your data platform as a graph of datasets (assets) and their relationships, then orchestrates the work needed to keep those datasets current.

Once an asset is created, Dagster does not automatically run the code for the asset, it must be [materialized](https://docs.dagster.io/guides/build/assets/configuring-assets) first. When an asset is materialized, Dagster runs the asset’s function and creates the asset. When a materialization begins, it kicks off a run. 

## Resources

A [resource](https://docs.dagster.io/dagster-basics-tutorial/resources#step-4-view-the-resource) in Dagster is something your assets need to do their work, but not the data you're producing. In the example of baking cookies, resources would be a mixing bowl, spoon, a baking sheet, and the oven.  
Resources often represent:  

* Database connections
* Data warehouse clients
* API clients
* Cloud storage clients
* Credentials and secrets
* Logging systems
* I/O managers
* Configuration dictionaries


## Schedules 

[Schedules](https://docs.dagster.io/guides/automate/schedules) define a fixed time interval to run your pipeline. In the example of the cookies, this could be planning to bake the cookies at 1 pm. 

## Asset Jobs

[Asset jobs](https://docs.dagster.io/guides/build/jobs/asset-jobs) execute and monitor specified assets. In the example of baking cookies, a job could be made to add the chocolate chips to the cookie dough once you have successfully made the cookie dough.

## Definitions

Each workflow (containing assets, resources or schedules) must be part of a [Definitions](https://docs.dagster.io/api/dagster/definitions#definitions) object. For CFA specifically, the definitions object is located within the [dagster_defs.py](https://github.com/CDCgov/cfa-dagster/blob/main/examples/dagster_defs.py) file. The `dagster_defs.py` file contains the Dagster-specific configurations, assets, jobs, and/or schedules in addition to the Definitions object. 

## Ops

[Ops](https://docs.dagster.io/guides/build/ops) are single units of work in the pipeline. In the cookie example, an op could be getting the mixing bowl out, getting the flour from the pantry, or picking up the whisk before mixing the ingredients together. Dagster treats each op as a managed step, which allows the user to track whether the step succeeded or failed, record logs and metadata, retry failed steps, and visualize the pipeline in the Dagster UI. 

## Sensors

[Sensors](https://docs.dagster.io/guides/automate/sensors#cursors-and-high-volume-events) check for events at regular time intervals, and if triggered, will start a job or other action. Sensors allow the workflow to run automatically without someone needing to manually start the pipeline. In the baking cookies example, this would be like having an assistant take the cookies out of the oven when the timer goes off. 

## Partitions

[Partitions](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-ops#non-partitioned-job-with-date-config) divide the workflow into smaller pieces, which could speed up computation by using parallel processing. Users can test on an individual partition before trying to run larger ranges of data. In the cookie example, this could be scaling the recipe back to make 8 cookies instead of making the cookie dough for 80 cookies. 

## Dynamic Graph Assets
[Dynamic graph assets](https://github.com/CDCgov/cfa-dagster/blob/main/src/cfa_dagster/dynamic_graph_asset.py) were developed specifically for CFA Dagster workflows and were built on existing Dagster concepts. In standard Dagster, if you want an asset to run multiple times with different configurations, you typically use partitions. `@dynamic_graph_asset` allows you to run the same logic across multiple independent dimensions in parallel.
