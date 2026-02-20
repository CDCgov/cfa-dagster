FROM ghcr.io/astral-sh/uv:debian

# set dagster home for the webserver
ENV DAGSTER_HOME=/opt/dagster/dagster_home

ARG WORKDIR=/opt/venv/infra
WORKDIR /opt/venv

# COPY cfa-dagster files
COPY ./src/ src/
COPY ./README.md .
COPY ./pyproject.toml .
COPY ./uv.lock .

WORKDIR ${WORKDIR}

# COPY infra files
COPY ./infra/pyproject.toml .
COPY ./infra/uv.lock .

# create a cirtual environment
ENV VIRTUAL_ENV=${WORKDIR}/.venv
RUN uv venv ${VIRTUAL_ENV}

# add python dependencies to PATH
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"

# get python dependencies
RUN uv sync

WORKDIR ${DAGSTER_HOME}
