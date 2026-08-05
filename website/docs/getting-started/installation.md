# Using Dagster in a Project

## Requirements

- **Python:** Dagster supports Python 3.10 - 3.13 (3.13 recommended).
- **Package manager:** To manage the python packages, we recommend [uv](https://docs.astral.sh/uv/) which Dagster uses internally.
- **Git:** Refer to the [Git documentation](https://github.com/git-guides/install-git) if you don’t have this installed.

## Python projects

To add Dagster to an existing python project, add a `dagster_defs.py` file to the root of your repo. You can add dependencies to the `pyproject.toml` like so:

```toml
[project]
name = "my-project"
requires-python = ">=3.13,<3.14"

# include cfa-dagster which bundles all other Dagster dependencies
dependencies = [
    "cfa-dagster",
]

# include cfa-dagster's dev dependencies like the dg cli and dagster websever
[dependency-groups]
dev = [
  "cfa-dagster[dev]",
]

# configure your dagster_defs.py file as a dagster project
# to allow use of the `dg` cli
[tool.dg]
directory_type = "project"

[tool.dg.project]
root_module = "dagster_defs"
defs_module = "dagster_defs"
code_location_target_module = "dagster_defs"
```

If your workflow uses Docker or Azure, you can include Dagster in your `Dockerfile` like so:

```docker
ARG WORKDIR=/app

# set an explicit workdir
WORKDIR ${WORKDIR}

# Dependency information
COPY pyproject.toml ./pyproject.toml
COPY uv.lock ./uv.lock

# Set VIRTUAL_ENV variable at runtime
ENV VIRTUAL_ENV=/cfa-stf-routine-forecasting/.venv

# Create the virtual environment
RUN uv venv "${VIRTUAL_ENV}"

# Update PATH to use the selected venv at runtime
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"

# Sync all python dependencies excluding dev dependencies to minimize image size
RUN uv sync --no-dev
```

## Non-python projects

If your repo does not use python or you do not want a `pyproject.toml` for other reasons, using Dagster is as easy as including a `dagster_defs.py` file in the root of your repo. With the `uv` package manager, you can include dependencies directly in the file:

```python
#!/usr/bin/env -S uv run --script
# PEP 723 dependency definition: https://peps.python.org/pep-0723/
# /// script
# requires-python = ">=3.13,<3.14"
# dependencies = [
#    "cfa-dagster[dev] @ git+https://github.com/cdcgov/cfa-dagster.git",
# ]
# ///
import dagster as dg
from cfa_dagster import (
   start_dev_env,
...
<remaining python code below>
```

If your workflow uses Docker or Azure, you can include Dagster in your `Dockerfile` like so:

```docker

# add Dagster workflow file
ARG WORKDIR=/app

# set an explicit workdir
WORKDIR ${WORKDIR}

# copy the Dagster definitions into the image
COPY ./dagster_defs.py .

# remove dev dependencies before install
RUN sed -i 's/cfa-dagster\[[^]]*\]/cfa-dagster/' dagster_defs.py

# create a virtual environment for the Dagster workflows
ENV VIRTUAL_ENV=${WORKDIR}/.venv
RUN uv venv ${VIRTUAL_ENV}

# install the Dagster workflow dependencies
RUN uv sync --script dagster_defs.py --active

# add the Dagster workflow dependencies to the system path
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"

```

## Using the Dagster CLI

The [Dagster CLI](https://docs.dagster.io/api/clis/dg-cli/dg-cli-reference) is a set of commands you can run directly in your terminal or shell to interact with the Dagster platform without using a web browser. A command-line interface is a text-based interface where users type commands to perform tasks. In Dagster’s case, the CLI allows you to:

- Manage and run jobs — start, stop, or list runs, view logs, and check run status.
- Work with assets — list assets, materialize them, or check their health.
- Debug issues — export or import run artifacts for troubleshooting.
- Validate definitions — check your Dagster code for errors before running.
- Manage deployments — list deployments, filter runs by deployment, and view branch-specific logs.
- Authenticate and configure — log in to your Dagster+ deployment, switch profiles, and store credentials securely. (not relevant for `cfa-dagster` users)

Dagster currently offers several CLIs for interacting with your definitions from the command line. Some of the actions require configuration, which `cfa-dagster` provides by re-exporting Dagster's CLIs with a `cfa-` prefix:

### The `dg` CLI (`cfa-dg`)

The `dg` CLI can be used to:

- Run a local dev server: `cfa-dg dev`
- Create and manage runs: `cfa-dg launch --asset my_asset`
- Validate definitions: `dg check defs`
- See more at the [docs](https://docs.dagster.io/api/clis/dg-cli/dg-cli-reference)

### The `dagster-webserver` CLI (`cfa-dagster-webserver`)

The `dagster-webserver` CLI can be used to run the Dagster websever against a set of definitions without running the background daemon processes. This is useful when you want to browse the Dagster run history without interfering with live runs. By providing a `DAGSTER_USER` environment variable, you can browse the run history of another user e.g.:

- `DAGSTER_USER=ap82 cfa-dagster-webserver`: view the run history for Giovanni Rella
- `DAGSTER_USER=github_actions cfa-dagster-webserver`: view the run history for Dagster runs launched in GitHub actions

See more at the [docs](https://docs.dagster.io/api/clis/cli#dagster-webserver)

### the `dagster` CLI (`cfa-dagster`)

The `dagster` CLI has been superseded `dg` CLI in most user-facing cases. `cfa-dagster` uses it under the hood to host the code locations. Docs are available [here](https://docs.dagster.io/api/clis/cli)
