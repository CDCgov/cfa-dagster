# CFA County Rt (Sub-state Rt)

This CFA project implements and evaluates hierarchical generalized additive models (HGAMs) for $R_t$ estimation at the county level in the United States. Click [here](https://github.com/cdcent/cfa-county-rt) to go to the GitHub repository.

## Dagster implementation

### Why Dagster

- The workflow includes reading in the most recent data from two APIs, running the model over specified states and diseases (Covid-19, RSV, influenza) whenever new data is available, performing post-processing on the model outputs, and updating a GitHub Pages website.
- If any step fails, the rest of the workflow does not continue and detailed logs for the specific disease and state allow the user to debug.

### Dagster details

- Implements an Azure Batch configuration if the user wants to run the workflow through production, otherwise, it is run locally through Docker.
- Many of the assets are "eager", meaning that they materialize as soon as the asset upstream successfully materializes.
- Assets include county Rt assets are separated by disease to generate one compute task per state, report date, and disease, and an R script to run the model for all states and all diseases and aggregate the results at the national-level.
- For partitioned assets (cfa_county_rt\_\*), a launchpad opens up allowing the user to specify the report date and state. There is an additional Config dropdown within Dagster to apply additional config.
- Weekly production run is scheduled for Wednesdays 6:30 AM EST.
