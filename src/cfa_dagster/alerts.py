"""Dagster alerting resources."""

from __future__ import annotations

import json
from collections.abc import Sequence
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import dagster as dg
from azure.identity import DefaultAzureCredential
from pydantic import Field

MICROSOFT_GRAPH_SCOPE = "https://graph.microsoft.com/.default"
MICROSOFT_GRAPH_SEND_MAIL_URL = (
    "https://graph.microsoft.com/v1.0/users/{sender}/sendMail"
)


class Microsoft365Resource(dg.ConfigurableResource):
    """Send e-mail notifications through M365 auth'ing with Default Azure Credential.
    """

    sender: str = Field(description="Microsoft 365 mailbox to send from")
    recipients: list[str] = Field(
        min_length=1,
        description="Default email recipients",
    )
    subject_prefix: str = "[Dagster]"
    graph_url: str = MICROSOFT_GRAPH_SEND_MAIL_URL
    timeout_seconds: float = Field(default=30, gt=0)

    def send_email(
        self,
        *,
        subject: str,
        body: str,
        recipients: Sequence[str] | None = None,
        html: bool = False,
    ) -> None:
        """Send one email using Microsoft Graph's ``sendMail`` endpoint."""
        if not subject.strip():
            raise ValueError("Email subject cannot be empty")
        if not body.strip():
            raise ValueError("Email body cannot be empty")

        to = list(recipients or self.recipients)
        if not to or any(not address.strip() for address in to):
            raise ValueError("Email recipients cannot be empty")

        credential = DefaultAzureCredential()
        token = credential.get_token(MICROSOFT_GRAPH_SCOPE).token
        payload = {
            "message": {
                "subject": f"{self.subject_prefix} {subject}".strip(),
                "body": {
                    "contentType": "HTML" if html else "Text",
                    "content": body,
                },
                "toRecipients": [
                    {"emailAddress": {"address": address}} for address in to
                ],
            },
            "saveToSentItems": False,
        }
        request = Request(
            self.graph_url.format(sender=self.sender),
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                # Graph returns 202 Accepted with an empty response body.
                if response.status != 202:
                    raise RuntimeError(
                        "Microsoft Graph returned unexpected HTTP status "
                        f"{response.status}"
                    )
        except HTTPError as error:
            # Do not include response bodies: they can contain mailbox details.
            raise RuntimeError(
                f"Microsoft Graph sendMail failed with HTTP {error.code}"
            ) from error


@dg.failure_hook(required_resource_keys={"microsoft365"})
def microsoft365_failure_hook(context: dg.HookContext) -> None:
    """Send a concise notification when a Dagster op fails."""
    event = context.failure_event
    error = getattr(event.event_specific_data, "error", None)
    error_text = error.to_string() if error is not None else "Unknown error"
    op_name = getattr(context.op, "name", "unknown op")
    context.resources.microsoft365.send_email(
        subject=f"Run failed: {context.job_name}",
        body=(
            f"Job: {context.job_name}\n"
            f"Op: {op_name}\n"
            f"Run ID: {context.run_id}\n\n"
            f"{error_text}"
        ),
    )
