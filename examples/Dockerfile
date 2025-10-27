FROM docker.io/rocker/r-ver:4.4.0


# We need curl to get UV and git to get a python dependency from GitHub
RUN apt-get update && apt-get install -y curl git

# install azure cli to use cli auth from host machine
# not necessary if running from the VAP
ARG INSTALL_AZ_CLI=false
RUN if [ "${INSTALL_AZ_CLI}" = "true" ]; then curl -sL https://aka.ms/InstallAzureCLIDeb | bash; fi

ARG WORKDIR=/app
WORKDIR ${WORKDIR}

# copy your application code
COPY ./hello.R .

# install uv and add to PATH
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

# add Dagster workflow files
ARG DAGSTER_WORKFLOW_DIR=cfa_dagster_sandbox
WORKDIR ${WORKDIR}/${DAGSTER_WORKFLOW_DIR}
COPY ./${DAGSTER_WORKFLOW_DIR}/dagster_defs.py .
COPY ./${DAGSTER_WORKFLOW_DIR}/pyproject.toml .

ENV VIRTUAL_ENV=${WORKDIR}/${DAGSTER_WORKFLOW_DIR}/.venv
# create a virtual environment for the dagster workflows
RUN uv venv ${VIRTUAL_ENV}

# back to the parent dir
WORKDIR ${WORKDIR}
# install the dagster workflow dependencies
RUN uv pip install -e ${DAGSTER_WORKFLOW_DIR}
# add the dagster workflow dependencies to the system path
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"
