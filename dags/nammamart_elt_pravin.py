"""NammaMart ELT pipeline (Part B): Extract -> Load RAW (text) -> Transform in the warehouse with SQL.

Layers in include/warehouse_pravin.duckdb:  raw_*  ->  stg_*  ->  mart_*   (each transform is a .sql file in include/sql/)
Incremental load: orders_2026-10-01_increment.csv is appended to raw_orders and MERGEd into stg_orders on order_id.
Schedule: 09:00 IST, Monday to Saturday.
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
from nammamart import common, elt  # noqa: E402

DAG_ID = common.DAG_ELT

default_args = {
    "owner": "pravin",
    "on_failure_callback": notify_failure,         # bonus: Slack alert, webhook kept in an Airflow Variable
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
}


def _rid() -> str:
    return get_current_context()["run_id"]


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nammamart", "elt", "pravin"],
    doc_md=__doc__,
)
def nammamart_elt_pravin():
    # ---- Extract: five parallel tasks (file present + schema contract check)
    def make_extract(name: str):
        @task(task_id=f"extract_{name}")
        def _extract() -> dict:
            return elt.extract(common.get_settings(), _rid(), name)
        return _extract()

    extracts = {n: make_extract(n) for n in common.FILES}

    # ---- Load RAW: exact copy, all text. (DuckDB allows one writer, so the loads run in one task.)
    @task
    def load_raw(extracted: dict) -> dict:
        return elt.load_raw(common.get_settings(), _rid(), extracted)

    # ---- Staging (SQL)
    def sql_task(task_id: str, filename: str):
        @task(task_id=task_id)
        def _t() -> dict:
            cfg = common.get_settings()
            elt.run_sql_file(cfg, _rid(), filename)
            common.write_stats(cfg, DAG_ID, _rid(), task_id)
            return {"sql": filename}
        return _t()

    stg_customers = sql_task("stg_customers", "10_stg_customers.sql")
    stg_products = sql_task("stg_products", "11_stg_products.sql")
    stg_stores = sql_task("stg_stores", "12_stg_stores.sql")

    @task
    def stg_orders() -> dict:
        cfg = common.get_settings()
        elt.run_sql_file(cfg, _rid(), "20_view_orders.sql")
        elt.run_sql_file(cfg, _rid(), "21_stg_orders.sql")
        common.write_stats(cfg, DAG_ID, _rid(), "stg_orders")
        return {"sql": "21_stg_orders.sql"}

    # ---- Incremental load: append the next-day file(s) to raw, then MERGE into staging
    @task
    def load_raw_increment() -> dict:
        return elt.load_raw_increment(common.get_settings(), _rid())

    @task
    def merge_stg_orders() -> dict:
        return elt.merge_stg_orders(common.get_settings(), _rid())

    stg_payments = sql_task("stg_payments", "30_stg_payments.sql")

    # ---- Quality scorecard
    @task
    def dq_results() -> dict:
        return elt.dq_results(common.get_settings(), _rid())

    # ---- Marts (SQL), one task per table
    def mart_task(filename: str):
        task_id = filename.split(".")[0].split("_", 1)[1]
        @task(task_id=task_id)
        def _m() -> dict:
            return elt.mart(common.get_settings(), _rid(), filename)
        return _m()

    # ---- Run audit (always runs)
    @task(trigger_rule="all_done")
    def run_audit() -> dict:
        ctx = get_current_context()
        start = getattr(ctx.get("dag_run"), "start_date", None)
        row = common.write_audit(common.get_settings(), DAG_ID, ctx["run_id"], elt.ELT_TASKS, start)
        if row["status"] != "success":
            # the audit row is already written; now make the DAG RUN red too. Without this the run would be
            # green (run_audit is the only final task and always succeeds) even if extract_products failed.
            raise AirflowFailException(f"pipeline incomplete, audit status: {row['status']}")
        return {k: str(v) for k, v in row.items()}

    raw = load_raw(extracts)
    chain_tasks = [raw, stg_customers, stg_products, stg_stores, stg_orders(), load_raw_increment(),
                   merge_stg_orders(), stg_payments, dq_results()]
    marts = [mart_task(f) for f in elt.MART_FILES]
    chain_tasks += marts
    for a, b in zip(chain_tasks, chain_tasks[1:]):
        a >> b
    [*extracts.values(), *chain_tasks] >> run_audit()


nammamart_elt_pravin()
