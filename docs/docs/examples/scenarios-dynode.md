# Scenarios DynODE Experiments

The CFA DynODE Experiments builds on the Scenario team's ODE Modeling Framework and can be scoped based on an endpoint (e.g., a report we are producing), or based on an idea or method change (e.g., what happens if we get rid of age structure). This repository is where code for experiments that use DynODE (<https://github.com/cdcgov/DynODE>) should live.  Whereas DynODE should change infrequently and backward compatibility is a priority, projects that use DynODE often are fluid in design and scope, and have idiosyncratic data requirements. Click [here](https://github.com/cdcent/DynODE-Experiments/tree/main) to go to the GitHub repository.


## Dagster implementation

Click [here](https://github.com/cdcent/DynODE-Experiments/pull/84/changes?diff=split#diff-72e262b4ba7124917f33b8e31970cf81d8d64f6baa77a9fad759a11b7e074d4c) to view the pull request for the DynODE Experiments repo to implement Dagster.

### Why Dagster

* The workflow runs in parallel over all states.
* The post-processing scripts depend on the outputs from the experiment script and sometimes some of the post-processing scripts depend on previous post-processing scripts. 

### Dagster details

* Configuration runs launcher to launch each run in an Azure CAJ and configures an executor to run each workflow steps in a new Azure Batch task for maximum scale. 
* Uses dynamic graph assets to run workflow in parallel over all states. 
* Assets include running an experiment, post-processing a plot to show the observed cases of Covid-19 versus the fitted cases, a post-processing script to read posterior and prior samples from state folders, and a post-processing script to create median maps.
* Jobs include building a Docker image and set various configuration settings.
* Ops include allowing the user to run the container previously built and explore the filesystem that will be used by Dagster and sending an autoscale requeue request to Azure Batch.