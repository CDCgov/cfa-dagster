FROM ghcr.io/astral-sh/uv:debian

ARG WORKDIR=/app

WORKDIR ${WORKDIR}

ENV VIRTUAL_ENV=${WORKDIR}/.venv

RUN uv venv ${VIRTUAL_ENV}

ENV PATH="${VIRTUAL_ENV}/bin:$PATH"

COPY ./dagster_defs.py .

RUN uv sync --script dagster_defs.py --active
