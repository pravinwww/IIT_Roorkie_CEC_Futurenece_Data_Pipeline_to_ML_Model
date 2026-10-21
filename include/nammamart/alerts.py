"""Failure alert (bonus): Slack message when a task fails, webhook kept in an Airflow Variable.

The webhook URL is never in code or in git. Set it in the Airflow UI: Admin > Variables >
`nammamart_slack_webhook` (or the env var NAMMAMART_SLACK_WEBHOOK for local tests).
If it is not set, the alert is only written to the task log, so the DAG still works without Slack.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request

log = logging.getLogger(__name__)


def _webhook() -> str | None:
    url = os.environ.get("NAMMAMART_SLACK_WEBHOOK")
    if url:
        return url
    try:
        try:
            from airflow.sdk import Variable
        except ImportError:
            from airflow.models import Variable
        return Variable.get("nammamart_slack_webhook", default=None)
    except Exception:  # no Airflow / no Variable store
        return None


def build_message(context: dict) -> str:
    ti = context.get("task_instance")
    dag_id = getattr(ti, "dag_id", None) or context.get("dag").dag_id
    task_id = getattr(ti, "task_id", "?")
    run_id = context.get("run_id", "?")
    err = str(context.get("exception", ""))[:300]
    return (f":red_circle: NammaMart pipeline failed: {dag_id} / task `{task_id}` / run {run_id}\n"
            f"Error: {err}\nThe 9 AM numbers may be stale: check the Airflow UI before using them.")


def notify_failure(context: dict) -> None:
    """Airflow on_failure_callback. Never raises (an alert must not break the pipeline)."""
    try:
        msg = build_message(context)
        log.error("ALERT: %s", msg)
        url = _webhook()
        if not url:
            log.warning("nammamart_slack_webhook not set: alert logged only")
            return
        req = urllib.request.Request(url, data=json.dumps({"text": msg}).encode(),
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as exc:  # noqa: BLE001
        log.error("alert could not be sent: %s", exc)
