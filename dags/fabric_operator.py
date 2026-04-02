"""
Custom Fabric notebook operator using MSI (Managed Service Identity).

Replaces FabricRunItemOperator from apache-airflow-providers-microsoft-fabric,
which requires service principal credentials not available in this environment.

Authentication: IMDS endpoint (http://169.254.169.254/metadata/token) — available
automatically in Fabric's managed Airflow; no credentials needed.
"""
import time
import requests
from airflow.models import BaseOperator


_FABRIC_RESOURCE = "https://api.fabric.microsoft.com"
_FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
_IMDS_URL = "http://169.254.169.254/metadata/token"
_POLL_INTERVAL_SECONDS = 15
_TERMINAL_STATUSES = {"completed", "succeeded", "success", "failed", "cancelled", "canceled", "deduped"}
_FAILED_STATUSES = {"failed", "cancelled", "canceled"}


def _get_msi_token() -> str:
    resp = requests.get(
        _IMDS_URL,
        params={"api-version": "2018-02-01", "resource": _FABRIC_RESOURCE},
        headers={"Metadata": "true"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


class FabricRunItemOperator(BaseOperator):
    """
    Triggers a Fabric item job (e.g. RunNotebook) and polls until terminal state.

    Drop-in replacement for apache_airflow_microsoft_fabric_plugin.operators.fabric.FabricRunItemOperator.
    Unused params (fabric_conn_id, deferrable) are accepted and silently ignored for compatibility.
    """

    ui_color = "#faebd4"
    ui_fgcolor = "#4d2c00"

    def __init__(
        self,
        *,
        workspace_id: str,
        item_id: str,
        job_type: str = "RunNotebook",
        job_params: dict | None = None,
        # kept for interface compatibility — not used
        fabric_conn_id: str | None = None,
        deferrable: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.job_type = job_type
        self.job_params = job_params or {}

    def execute(self, context):
        token = _get_msi_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        trigger_url = (
            f"{_FABRIC_API_BASE}/workspaces/{self.workspace_id}"
            f"/items/{self.item_id}/jobs/instances?jobType={self.job_type}"
        )
        self.log.info("Triggering Fabric job: %s", trigger_url)
        resp = requests.post(trigger_url, headers=headers, json=self.job_params, timeout=30)
        resp.raise_for_status()

        location = resp.headers.get("Location") or resp.headers.get("location")
        if not location:
            self.log.info("No Location header — job triggered (fire-and-forget mode)")
            return

        self.log.info("Polling job status at: %s", location)
        while True:
            time.sleep(_POLL_INTERVAL_SECONDS)

            token = _get_msi_token()
            headers["Authorization"] = f"Bearer {token}"

            poll = requests.get(location, headers=headers, timeout=30)
            poll.raise_for_status()
            data = poll.json()
            status = data.get("status", "").lower()
            self.log.info("Job status: %s", status)

            if status in _TERMINAL_STATUSES:
                if status in _FAILED_STATUSES:
                    raise RuntimeError(
                        f"Fabric job failed. item_id={self.item_id} status={status} "
                        f"response={data}"
                    )
                return data
