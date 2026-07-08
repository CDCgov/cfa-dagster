# Using Dagster in a Project

* **Python:** Dagster supports Python 3.10 - 3.13 (3.12 recommended).
* **Package manager:** To manage the python packages, we recommend [uv](https://docs.astral.sh/uv/) which Dagster uses internally.
* **Git:** Refer to the [Git documentation](https://github.com/git-guides/install-git) if you don’t have this installed.


## Install Dagster
1. Activate your Python [virtual environment]( https://docs.python.org/3/library/venv.html)  
2. [Install Dagster with uv:]( https://docs.dagster.io/getting-started/installation#installation-requirements-for-manually-creating-or-updating-a-project)  
`uv add dagster dagster-webserver dagster-dg-cli`.   
3. Clone the [`cfa-dagster` repository](https://github.com/CDCgov/cfa-dagster/tree/main):  
`git clone https://github.com/CDCgov/cfa-dagster.git`  
Or  
`gh repo clone cdcgov/cfa-dagster` 

## Using the Dagster Command-Line Interface (CLI)
The [Dagster CLI](https://docs.dagster.io/api/clis/dg-cli/dg-cli-reference) is a set of commands you can run directly in your terminal or shell to interact with the Dagster platform without using a web browser. A command-line interface is a text-based interface where users type commands to perform tasks. In Dagster’s case, the CLI allows you to:

* Manage and run jobs — start, stop, or list runs, view logs, and check run status.
* Work with assets — list assets, materialize them, or check their health.
* Debug issues — export or import run artifacts for troubleshooting.
* Validate definitions — check your Dagster code for errors before running.
* Manage deployments — list deployments, filter runs by deployment, and view branch-specific logs.
* Authenticate and configure — log in to your Dagster+ deployment, switch profiles, and store credentials securely

Instructions for installing and configuring the Dagster CLI from the Dagster official documentation can be found [here](https://docs.dagster.io/api/clis/dg-cli/dg-cli-configuration). 

### Running the `cfa-dagster` CLI
1. Activate your virtual environment (`venv`)
2. Add `cfa-dagster` to `pyproject.toml`
```toml
 dependencies = [
    "cfa-dagster @ git+https://github.com/cdcgov/cfa-dagster.git",
 ]

```
3. `uv sync`
4. Run `cfa-dg dev`

## Update your Dockerfile

After your virtual environment is activated in your Dockerfile, add the following code:

```
# add Dagster workflow file
COPY ./dagster_defs.py .

# install the dagster workflow dependencies
RUN uv sync --script dagster_defs.py --active
```

## Logging in with Azure

You will need to log in with Azure, so check out the [Predict Handbook Site](https://potential-adventure-637em36.pages.github.io/azure/AzureBasics-login_methods.html#primary-login-methods) for details on how to do so. 