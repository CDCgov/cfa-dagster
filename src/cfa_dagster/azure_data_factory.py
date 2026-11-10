import json
import logging
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import dagster as dg
from azure.core.exceptions import AzureError
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pydantic import Field

ADF_API_VERSION = "2018-06-01"
ARM_SCOPE = "https://management.azure.com/.default"
TRANSIENT_HTTP_CODES = {408, 429, 500, 502, 503, 504}


@dataclass(frozen=True)
class ADFPipelineRun:
    run_id: str
    status: str
    studio_url: str

    def metadata(self) -> dict[str, Any]:
        return {
            "adf_run_id": self.run_id,
            "adf_status": self.status,
            "adf_studio_url": dg.MetadataValue.url(self.studio_url),
        }


class ADFResource(dg.ConfigurableResource):
    """Non-secret ADF configuration; credentials are fetched only during execution."""

    vault_url: str = "https://cfa-common-adf-prd.vault.azure.net/"
    subscription_secret: str = "CFA-EDAV-Subscription-ID"
    tenant_secret: str = "CFA-EDAV-Tenant-ID"
    client_id_secret: str = "CFA-DataShare-PRD-SP-Client-ID"
    client_secret_secret: str = "SP-CFA-To-EDAV-DataShare-Batch-Pipeline-Secret"
    resource_group: str = "ocio-dav-prd"
    factory_name: str = "edav-cfa-prd-factory"
    poll_interval_seconds: float = Field(default=15, gt=0)
    wait_timeout_seconds: float = Field(default=1800, gt=0)
    http_timeout_seconds: float = Field(default=30, gt=0)

    def run_pipeline(
        self,
        *,
        pipeline_name: str,
        log: logging.Logger | dg.DagsterLogManager,
        parameters: Mapping[str, Any] | None = None,
    ) -> ADFPipelineRun:
        """Trigger any pipeline and wait for success; parameters are not logged."""
        return run_adf_pipeline(
            self,
            pipeline_name=pipeline_name,
            parameters=parameters,
            log=log,
        )


class ADFDatalakeToMUSTResource(ADFResource):
    """Typed adapter for ext_datalake_to_must to transfer data from the cfadatalakeprd storage account in the Azure EXT subscription to the cdc.gov MUST drive; callers configure their own root."""

    base_destination_directory: str = Field(
        min_length=1,
        description="Base MUST destination directory; copy destinations are suffixes.",
    )

    def copy_to_must(
        self,
        *,
        source_container: str,
        source_file_directory: str,
        destination_file_directory: str,
        log: logging.Logger | dg.DagsterLogManager,
        should_delete_after_copy: bool = False,
    ) -> ADFPipelineRun:
        """Validate copy arguments and delegate execution to the generic resource."""
        for name, value in (
            ("source_container", source_container),
            ("source_file_directory", source_file_directory),
            ("destination_file_directory", destination_file_directory),
            ("base_destination_directory", self.base_destination_directory),
        ):
            if not isinstance(value, str) or not value.strip():
                raise dg.Failure(f"ADF {name} cannot be empty.", allow_retries=False)
        if not isinstance(should_delete_after_copy, bool):
            raise dg.Failure(
                "ADF should_delete_after_copy must be a boolean.", allow_retries=False
            )
        destination = (
            self.base_destination_directory.rstrip("\\")
            + "\\"
            + destination_file_directory.lstrip("\\")
        )
        return self.run_pipeline(
            pipeline_name="ext_datalake_to_must",
            parameters={
                "source_container": source_container,
                "source_file_directory": source_file_directory,
                "destination_file_directory": destination,
                "should_delete_after_copy": should_delete_after_copy,
            },
            log=log,
        )


def _request_json(
    url: str,
    credential: ClientSecretCredential,
    timeout: float,
    parameters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request = Request(
        url,
        data=json.dumps(parameters).encode() if parameters is not None else None,
        headers={
            "Authorization": f"Bearer {credential.get_token(ARM_SCOPE).token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        result = json.load(response)
    if not isinstance(result, dict):
        raise ValueError("ADF response is not a JSON object")
    return result


def _redact_error_text(text: str, client_secret: str) -> str:
    for value in (
        client_secret,
        json.dumps(client_secret)[1:-1],
        quote(client_secret, safe=""),
    ):
        text = text.replace(value, "[REDACTED]")
    text = re.sub(r"(?i)\bBearer\s+[^\s\"'<>]+", "Bearer [REDACTED]", text)
    text = re.sub(r"\beyJ[\w-]*\.[\w-]+\.[\w-]+\b", "[REDACTED]", text)
    return re.sub(
        r"""(?i)(["']?(?:access_token|refresh_token|client_secret|authorization)["']?\s*[:=]\s*["']?)[^"'\s,}]+""",
        r"\1[REDACTED]",
        text,
    )


def _error_details(error: Exception, client_secret: str) -> tuple[str, dict[str, Any]]:
    """Preserve Azure diagnostics without logging request headers or credentials."""
    summary = _redact_error_text(str(error), client_secret)
    metadata: dict[str, Any] = {"adf_error_type": type(error).__name__}
    if isinstance(error, HTTPError):
        metadata["adf_http_status"] = error.code
        summary = (
            f"HTTP {error.code}: {_redact_error_text(str(error.reason), client_secret)}"
        )
        if error.headers:
            for header in ("x-ms-request-id", "x-ms-correlation-request-id"):
                value = error.headers.get(header)
                if value:
                    metadata[header] = _redact_error_text(value, client_secret)
        try:
            body = error.read(65536)
        except (OSError, ValueError):
            body = b""
        finally:
            error.close()
        if body:
            body = body.decode("utf-8", errors="replace")
            # Redact before parsing too, so nested details cannot leak credentials.
            body = _redact_error_text(body, client_secret)
            metadata["adf_response_body"] = body[:8000]
            try:
                payload = json.loads(body)
            except ValueError:
                summary += f"; {body[:2000]}"
            else:
                details = (
                    payload.get("error", payload)
                    if isinstance(payload, dict)
                    else payload
                )
                if isinstance(details, dict):
                    if details.get("code"):
                        metadata["adf_error_code"] = str(details["code"])
                        summary += f"; {details['code']}"
                    if details.get("message"):
                        summary += f"; {str(details['message'])[:4000]}"
    metadata["adf_error_message"] = summary
    return summary, metadata


def run_adf_pipeline(
    config: ADFResource,
    *,
    pipeline_name: str,
    log: logging.Logger | dg.DagsterLogManager,
    parameters: Mapping[str, Any] | None = None,
) -> ADFPipelineRun:
    """Trigger once, then poll to completion without automatically cancelling ADF."""
    if not isinstance(pipeline_name, str) or not pipeline_name.strip():
        raise dg.Failure("ADF pipeline name cannot be empty.", allow_retries=False)
    if parameters is not None and (
        not isinstance(parameters, Mapping)
        or any(not isinstance(key, str) for key in parameters)
    ):
        raise dg.Failure(
            "ADF parameters must be a mapping with string keys.", allow_retries=False
        )
    parameters = dict(parameters) if parameters is not None else {}
    try:
        json.dumps(parameters, allow_nan=False)
    except (TypeError, ValueError, OverflowError, RecursionError):
        # Do not include parameter values or serialization exceptions in diagnostics.
        raise dg.Failure(
            "ADF parameters must contain JSON-serializable values.", allow_retries=False
        ) from None

    with (
        DefaultAzureCredential() as vault_credential,
        SecretClient(vault_url=config.vault_url, credential=vault_credential) as vault,
    ):
        secrets = {}
        for key, name in (
            ("subscription", config.subscription_secret),
            ("tenant", config.tenant_secret),
            ("client_id", config.client_id_secret),
            ("client_secret", config.client_secret_secret),
        ):
            value = vault.get_secret(name).value
            if not value:
                raise dg.Failure(
                    f"Key Vault secret {name!r} is empty.", allow_retries=False
                )
            secrets[key] = value

    factory_id = (
        f"/subscriptions/{secrets['subscription']}/resourceGroups/{config.resource_group}"
        f"/providers/Microsoft.DataFactory/factories/{config.factory_name}"
    )
    studio_query = urlencode({"factory": factory_id})
    studio_url = f"https://adf.azure.com/?{studio_query}"
    factory_url = f"https://management.azure.com{quote(factory_id, safe='/')}"
    api_query = f"?api-version={ADF_API_VERSION}"
    with ClientSecretCredential(
        tenant_id=secrets["tenant"],
        client_id=secrets["client_id"],
        client_secret=secrets["client_secret"],
    ) as credential:
        # Never retry createRun: a timeout can mean ADF accepted the request.
        try:
            response = _request_json(
                f"{factory_url}/pipelines/{quote(pipeline_name, safe='')}/createRun{api_query}",
                credential,
                config.http_timeout_seconds,
                parameters,
            )
            run_id = response.get("runId")
            if not isinstance(run_id, str) or not run_id:
                raise ValueError("ADF response has no run ID")
        except (HTTPError, URLError, OSError, ValueError, AzureError) as error:
            details, error_metadata = _error_details(error, secrets["client_secret"])
            log.error(f"ADF trigger failed: {details}")
            raise dg.Failure(
                f"ADF trigger did not return a valid run ID. {details}. The request may have been "
                "accepted; check ADF Studio before retrying.",
                metadata={"adf_studio_url": dg.MetadataValue.url(studio_url)}
                | error_metadata,
                allow_retries=False,
            ) from None

        studio_url = (
            f"https://adf.azure.com/en/monitoring/pipelineruns/{quote(run_id, safe='')}"
            f"?{studio_query}"
        )
        log.info(f"Started ADF pipeline {pipeline_name}; run ID: {run_id}")
        log.info(f"ADF run monitoring: {studio_url}")
        run_url = f"{factory_url}/pipelineruns/{quote(run_id, safe='')}{api_query}"
        deadline = time.monotonic() + config.wait_timeout_seconds
        last_status = "Unknown"
        while True:
            run = ADFPipelineRun(run_id, last_status, studio_url)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise dg.Failure(
                    f"Timed out waiting for ADF run {run_id}. The run was not cancelled; "
                    "check ADF Studio before retrying.",
                    metadata=run.metadata(),
                    allow_retries=False,
                )
            try:
                response = _request_json(
                    run_url, credential, min(config.http_timeout_seconds, remaining)
                )
            except (URLError, OSError) as error:
                # Only the read-only status request is eligible for retries.
                details, error_metadata = _error_details(
                    error, secrets["client_secret"]
                )
                if (
                    isinstance(error, HTTPError)
                    and error.code not in TRANSIENT_HTTP_CODES
                ):
                    log.error(f"Cannot read ADF run {run_id}: {details}")
                    raise dg.Failure(
                        f"Cannot read ADF run {run_id}: {details}. "
                        "The run was not cancelled.",
                        metadata=run.metadata() | error_metadata,
                        allow_retries=False,
                    ) from None
                log.warning(
                    f"Transient error reading ADF run {run_id}: {details}; retrying status polling."
                )
            except (ValueError, AzureError) as error:
                details, error_metadata = _error_details(
                    error, secrets["client_secret"]
                )
                log.error(f"Cannot read ADF run {run_id}: {details}")
                raise dg.Failure(
                    f"Could not read a valid status for ADF run {run_id}: {details}. "
                    "The run was not cancelled.",
                    metadata=run.metadata() | error_metadata,
                    allow_retries=False,
                ) from None
            else:
                status = response.get("status")
                if not isinstance(status, str) or status not in {
                    "Queued",
                    "InProgress",
                    "Canceling",
                    "Succeeded",
                    "Failed",
                    "Cancelled",
                }:
                    raise dg.Failure(
                        f"Unrecognized status for ADF run {run_id}. Check ADF Studio.",
                        metadata=run.metadata(),
                        allow_retries=False,
                    )
                if status != last_status:
                    log.info(f"ADF run {run_id}: {status}")
                last_status = status
                run = ADFPipelineRun(run_id, status, studio_url)
                if status == "Succeeded":
                    return run
                if status in {"Failed", "Cancelled"}:
                    message = str(
                        response.get("message")
                        or "No pipeline-level error message was provided."
                    )
                    message = _redact_error_text(message, secrets["client_secret"])
                    raise dg.Failure(
                        f"ADF run {run_id} {status}: {message}",
                        metadata=run.metadata() | {"adf_message": message},
                        allow_retries=False,
                    )
            time.sleep(
                min(config.poll_interval_seconds, max(0, deadline - time.monotonic()))
            )
