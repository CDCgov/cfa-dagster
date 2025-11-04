FROM docker.io/rocker/r-ver:4.4.0


# We need curl to get UV and git to get a python dependency from GitHub
RUN apt-get update && apt-get install -y curl git

# install azure cli to use cli auth from host machine
# not necessary if running from the VAP
ARG INSTALL_AZ_CLI=false
RUN if [ "${INSTALL_AZ_CLI}" = "true" ]; then curl -sL https://aka.ms/InstallAzureCLIDeb | bash; fi

# WORKDIR MUST be /opt/dagster/code_location/<your repo name>
ARG WORKDIR=/opt/dagster/code_location/cfa-dagster
WORKDIR ${WORKDIR}

# copy your application code
COPY ./hello.R .

# install uv and add to PATH
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

# add Dagster workflow file
COPY ./dagster_defs.py .

# the virtual environment MUST be .venv in the same directory as your dagster workflow file
ENV VIRTUAL_ENV=${WORKDIR}/.venv
# create a virtual environment for the dagster workflows
RUN uv venv ${VIRTUAL_ENV}

# install the dagster workflow dependencies
RUN uv sync --script dagster_defs.py --active

# add the dagster workflow dependencies to the system path
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"
