"""
Airflow callbacks for Fabric Managed Airflow.
Sends failure/SLA alerts to Microsoft Teams via webhook.
"""
import json
import logging
import urllib.request

from airflow.models import Variable

log = logging.getLogger(__name__)


def _post_teams(webhook_url: str, payload: dict) -> None:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        if resp.status not in (200, 202):
            log.warning("Teams webhook returned status %s", resp.status)


def on_failure_teams_alert(context: dict) -> None:
    """
    Fires on task failure. Posts a Teams card with task name, DAG, run_id,
    try_number, and a link to the Airflow log.
    """
    webhook_url = Variable.get("teams_webhook_url", default_var=None)
    if not webhook_url:
        log.warning("teams_webhook_url Airflow variable not set — skipping alert")
        return

    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    run_id = ti.run_id
    try_number = ti.try_number
    log_url = ti.log_url

    payload = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "themeColor": "FF0000",
        "summary": f"❌ Task failed: {task_id}",
        "sections": [
            {
                "activityTitle": f"❌ **{task_id}** failed",
                "activitySubtitle": f"DAG: `{dag_id}` · Run: `{run_id}` · Try: {try_number}",
                "facts": [
                    {"name": "DAG", "value": dag_id},
                    {"name": "Task", "value": task_id},
                    {"name": "Run ID", "value": run_id},
                    {"name": "Try #", "value": str(try_number)},
                ],
                "markdown": True,
            }
        ],
        "potentialAction": [
            {
                "@type": "OpenUri",
                "name": "View Log",
                "targets": [{"os": "default", "uri": log_url}],
            }
        ],
    }

    try:
        _post_teams(webhook_url, payload)
        log.info("Teams alert sent for %s / %s", dag_id, task_id)
    except Exception as exc:
        log.warning("Failed to send Teams alert: %s", exc)


def on_sla_miss_teams_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Fires when a task misses its SLA window.
    """
    webhook_url = Variable.get("teams_webhook_url", default_var=None)
    if not webhook_url:
        return

    missed = ", ".join(str(s.task_id) for s in slas)
    payload = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "themeColor": "FFA500",
        "summary": f"⚠️ SLA miss: {missed}",
        "sections": [
            {
                "activityTitle": f"⚠️ SLA miss in **{dag.dag_id}**",
                "activitySubtitle": f"Tasks missed their SLA window: `{missed}`",
                "markdown": True,
            }
        ],
    }

    try:
        _post_teams(webhook_url, payload)
    except Exception as exc:
        log.warning("Failed to send SLA Teams alert: %s", exc)
