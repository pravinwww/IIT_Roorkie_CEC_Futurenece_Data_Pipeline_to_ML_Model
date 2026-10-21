"""NammaMart ETL pipeline (Part A): Extract -> Validate -> Quarantine -> Transform -> Load -> Reconcile -> Report.

Schedule: 09:00 IST, Monday to Saturday.  All logic lives in include/nammamart/etl.py so it can be tested
without Airflow (scripts/run_local.py). This file only wires the tasks together.
"""
import sys
from datetime import timedelta
from pathlib import Path

import pendulum

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))

try:  # Airflow 3
    from airflow.sdk import dag, get_current_context, task
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:  # Airflow 2
    from airflow.decorators import dag, task
    from airflow.exceptions import AirflowFailException
    from airflow.operators.python import get_current_context

from nammamart.alerts import notify_failure  # noqa: E402
from nammamart import common, etl  # noqa: E402

DAG_ID = common.DAG_ETL

default_args = {
    "owner": "pravin",
    "on_failure_callback": notify_failure,         # bonus: Slack alert, webhook kept in an Airflow Variable
    "retries": 2,                                  # transient problems (file still being written, DB lock)
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),    # a hung task must not block the 09:30 stand-up
}


def _run_id() -> str:
    return get_current_context()["run_id"]


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * 1-6",                                           # 09:00 Mon-Sat
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),     # timezone-aware: IST, not UTC
    catchup=False,                                                    # inputs are static files: no backfill
    max_active_runs=1,
    default_args=default_args,
    tags=["nammamart", "etl", "pravin"],
    doc_md=__doc__,
)
def nammamart_etl_pravin():
    # ---- 1. Extract: five parallel tasks, one per file
    def make_extract(name: str):
        @task(task_id=f"extract_{name}")
        def _extract() -> dict:
            return etl.extract(common.get_settings(), _run_id(), name)
        return _extract()

    extracts = {n: make_extract(n) for n in common.FILES}

    # ---- 2. Validate (data-quality rules, 6 dimensions) -> passed / failed rows
    @task
    def validate(extracted: dict) -> dict:
        return etl.validate(common.get_settings(), _run_id(), extracted)

    # ---- 3. Quarantine failed rows with a failure_reason
    @task
    def quarantine(validated: dict) -> dict:
        return etl.quarantine(common.get_settings(), _run_id(), validated)

    # ---- 4. Transform: standardise, dedupe, mask PII, join, derive columns
    @task
    def transform(validated: dict) -> dict:
        return etl.transform(common.get_settings(), _run_id(), validated)

    # ---- 5. Load star schema into DuckDB (idempotent upsert)
    @task
    def load(transformed: dict) -> dict:
        return etl.load(common.get_settings(), _run_id(), transformed)

    # ---- 6. Reconcile orders with payments
    @task
    def reconcile(extracted: dict, loaded: dict) -> dict:
        return etl.reconcile(common.get_settings(), _run_id(), extracted, loaded)

    # ---- 7. Report tables, printed in the task log
    @task
    def report(reconciled: dict) -> dict:
        return etl.report(common.get_settings(), _run_id())

    # ---- Run audit: always runs (all_done), one row per run in pipeline_runs
    @task(trigger_rule="all_done")
    def run_audit() -> dict:
        ctx = get_current_context()
        start = getattr(ctx.get("dag_run"), "start_date", None)
        row = common.write_audit(common.get_settings(), DAG_ID, ctx["run_id"], etl.ETL_TASKS, start)
        if row["status"] != "success":
            # the audit row is already written; now make the DAG RUN red too. Without this the run would be
            # green (run_audit is the only final task and always succeeds) even if extract_products failed.
            raise AirflowFailException(f"pipeline incomplete, audit status: {row['status']}")
        return {k: str(v) for k, v in row.items()}

    extracted = {n: t for n, t in extracts.items()}
    v = validate({n: t for n, t in extracted.items()})
    q = quarantine(v)
    t = transform(v)
    ld = load(t)
    rc = reconcile({n: t_ for n, t_ in extracted.items()}, ld)
    rp = report(rc)
    audit = run_audit()
    [*extracts.values(), v, q, t, ld, rc, rp] >> audit


nammamart_etl_pravin()
