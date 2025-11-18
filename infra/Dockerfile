FROM ghcr.io/astral-sh/uv:debian

# set dagster home for the webserver
ENV DAGSTER_HOME=/opt/dagster/dagster_home

ARG WORKDIR=/opt/venv
WORKDIR ${WORKDIR}

# Use the virtual environment automatically
ENV VIRTUAL_ENV=${WORKDIR}/.venv
RUN uv venv ${VIRTUAL_ENV}

# add python dependencies to PATH
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"


COPY ./pyproject.toml .
COPY ./uv.lock .

# get python dependencies
RUN uv sync

WORKDIR ${DAGSTER_HOME}
