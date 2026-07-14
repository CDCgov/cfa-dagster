# CFA NSSP ETL Pipeline

The CFA NSSp ETL pipeline pulls in respiratory viruses data daily from the NSSP API made available for CFA. Click [here](https://github.com/cdcent/cfa-nssp-etl) to go to the GitHub repository.

## Dagster implementation

### Why Dagster

* This pipeline requires updates to be made at regular intervals.
* Dagster allows for testing new code in Azure before it is merged into `main`.
* Data comes from two APIs and if the code to pull data from one API fails, it will fail for the other API, so Dagster stops the job from continuing and provides logs for why an asset did not materialize successfully.

### Dagster details

* Implements the Azure CAJ configuration, which uses the Azure Data Lake Storage Gen2 File System I/O Manager.
* Daily partitions allow for the process to pull the most recent daily data rather than all of the data available.
* Assets include NSSP Gold Data for the v1 API and NSSP Gold Data for the v2 API.
* Outputs are the file paths to the data stored in Azure.
