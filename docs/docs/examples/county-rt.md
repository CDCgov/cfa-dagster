# CFA County Rt (Sub-state Rt)

This CFA project implements and evaluates hierarchical generalized additive models (HGAMs) for \(R_t\) estimation at the county level in the
United States. Click [here](https://github.com/cdcent/cfa-county-rt) to go to the GitHub repository.

## Dagster implementation

### Why Dagster

For this project, Dagster orchestrates the modeling and post-processing workflow.  

### Dagster details from the GitHub repo

To run the code with [Dagster](https://docs.dagster.io/), run
`uv run dagster_defs.py` to start the Dagster
[webserver](http://127.0.0.1:3000/asset-groups).

The Dagster webserver will display 4 assets:

1.  cfa_county_rt_covid_19
2.  cfa_county_rt_influenza
3.  cfa_county_rt_rsv
4.  national_hgam

Materializing the cfa_county_rt\_\* assets is the equivalent of
`make run-prod` (runs all states and all diseases in Azure Batch production environment). Materializing the national_hgam asset is the equivalent
of `make run-national-prod` (runs an R script for all states and all diseases and aggregates at the national-level).

The county Rt assets are separated by disease to generate one compute
task per state, report date, and disease.

From the [Lineage](http://127.0.0.1:3000/asset-groups) page, you can
materialize one or more assets with the Materialize button. For
partitioned assets (cfa_county_rt\_\*), a launchpad will open up
allowing you to specify the report date and state. There is an
additional Config dropdown to apply additional config. For
non-partitioned assets (national_hgam), holding Shift + clicking
Materialize will give you a launchpad with default config applied.

Before running, choose your runtime environment by modifying the
`Definitions` class at the bottom of `dagster_defs.py`. This example is
running on Azure Batch in production and on Docker locally:

``` python
defs = dg.Definitions(
    ...,
    **(azure_batch_metadata if is_production else
       # uncomment below to run locally on docker
       docker_metadata
       # uncomment below to run on Azure Container App Job
       # azure_caj_metadata
       # uncomment below to run on Azure Batch
       # azure_batch_metadata
       )
)
```

For most runtime environments (docker, Batch, CAJ), you will need to
build (and potentially push) a Docker image which can be done with the
build image
[job](http://127.0.0.1:3000/locations/dagster_defs.py/jobs/job_build_image/playground).

The weekly production run is scheduled for Wednesdays 6:30 AM EST on the
[Automation](http://127.0.0.1:3000/locations/dagster_defs.py/schedules/job_county_rt_pipeline_schedule)
page.