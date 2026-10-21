"""Run Part F experiments 1-9 LOCALLY (no Airflow) to get the data-side results: row counts, quarantine reasons,
dq_results. What only Airflow can show (task states, retries, Clear) must be observed in the Airflow UI."""
import os
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
RUN = ROOT / "scripts" / "run_local.py"
DATA = ROOT / "include" / "data"
W = Path("/tmp/nm_experiments")


def run(db, data, rid, which="both", extra=()):
    env = dict(os.environ)
    p = subprocess.run([sys.executable, str(RUN), which, "--data-dir", str(data), "--db", str(db), "--quarantine-dir",
                        str(W / "q"), "--work-dir", str(W / "w"), "--run-id", rid, *extra], capture_output=True,
                       text=True, env=env)
    return p.returncode, (p.stdout + p.stderr)


def q(db, sql):
    c = duckdb.connect(str(db), read_only=True)
    try:
        return c.execute(sql).fetchall()
    finally:
        c.close()


def counts(db):
    return {"etl_fact_orders": q(db, "select count(*) from etl_fact_orders")[0][0],
            "raw_orders": q(db, "select count(*) from raw_orders")[0][0],
            "stg_orders_ok": q(db, "select count(*) from stg_orders where dq_status<>'QUARANTINE'")[0][0],
            "net_revenue_etl": q(db, "select round(sum(net_revenue),2) from etl_fact_orders")[0][0],
            "net_revenue_elt": q(db, "select round(sum(net_revenue),2) from stg_orders where dq_status<>'QUARANTINE'")[0][0]}


def dqrows(db, rid, like):
    return q(db, f"select dag_id, check_name, rows_failed, status from dq_results where run_id='{rid}' and ({like}) and rows_failed>0 order by 1,2")


def main():
    shutil.rmtree(W, ignore_errors=True)
    W.mkdir(parents=True)
    out = []
    P = out.append
    base = W / "base_data"
    base.mkdir()
    for f in ["orders", "payments", "customers", "products", "stores"]:
        shutil.copy(DATA / "nammamart" / f"{f}.csv", base / f"{f}.csv")
    db = W / "wh.duckdb"

    rc, log = run(db, base, "e0_prev")           # earlier normal run (gives the volume history)
    rc, log = run(db, base, "e1_happy")
    P(f"E1 happy path rc={rc} counts={counts(db)}")
    P("   quarantine by file: " + str(q(db, "select source_file, sum(rows_rejected) from etl_rejected_rows where run_id='e1_happy' group by 1 order by 1")))
    rc, log = run(db, base, "e2_rerun")
    shutil.copy(db, W / "baseline.duckdb")        # September-only baseline used by experiments 5-9
    P(f"E2 re-run rc={rc} counts={counts(db)}  (ETL fact unchanged; raw_orders grew by one batch)")
    shutil.copy(DATA / "nammamart" / "orders_2026-10-01_increment.csv", base / "orders_2026-10-01_increment.csv")
    rc, log = run(db, base, "e3_inc")
    P(f"E3 increment rc={rc} counts={counts(db)}")
    P("   " + [l for l in log.splitlines() if "load: fact" in l][0])
    P("   " + [l for l in log.splitlines() if "merge_stg_orders" in l][0])
    (base / "orders_2026-10-01_increment.csv").unlink()

    for name, rid in [("exp5_bad_rows", "e5"), ("exp6_schema_change", "e6"), ("exp7_volume_50_orders", "e7"),
                      ("exp8_drift_unit_price", "e8"), ("exp8b_drift_price_and_amount", "e8b"), ("exp9_stale_30_days", "e9")]:
        d = DATA / "experiments" / name
        edb = W / f"{rid}.duckdb"
        shutil.copy(W / 'baseline.duckdb', edb)  # September-only baseline warehouse
        extra = ["--volume-check"] if rid == "e7v" else ()
        rc, log = run(edb, d, rid, extra=extra)
        P(f"\n{name}: rc={rc}")
        if rc != 0:
            P("   FAILED: " + [l for l in log.splitlines() if "Error" in l or "schema" in l.lower()][-1][:240])
            continue
        P(f"   counts={counts(edb)}")
        if rid == "e5":
            P("   quarantine rows for ORDEXP*: " + str(q(edb, "select source_file,row_key,failure_reason from elt_quarantine where run_id='e5' and row_key like 'ORDEXP%' order by 2")))
            P("   ETL quarantine files: " + str(q(edb, "select source_file, failure_reason, rows_rejected from etl_rejected_rows where run_id='e5' and source_file='orders' order by 3 desc limit 25")))
        if rid in ("e7",):
            P("   volume rows with the check OFF: " + str(q(edb, "select dag_id,check_name,rows_checked,rows_failed,status from dq_results where run_id='e7' and check_name like 'orders.volume%' order by 1")))
            rc2, _ = run(edb, d, "e7v", extra=["--volume-check"])
            P("   with the check ON:            " + str(q(edb, "select dag_id,check_name,rows_checked,rows_failed,status from dq_results where run_id='e7v' and check_name like 'orders.volume%' order by 1")))
        if rid in ("e8", "e8b"):
            P("   category margin: " + str(q(edb, "select * from etl_category_margin order by 1")))
            P("   S05 store revenue in mart: " + str(q(edb, "select store_id, round(sum(net_revenue),2) from mart_daily_store_kpis where store_id='S05' group by 1")))
            P("   quarantined orders for S05 by reason: " + str(q(edb, "select failure_reason, count(*) from elt_quarantine where run_id='%s' and source_file='orders' group by 1 order by 2 desc limit 5" % rid)))
        if rid == "e9":
            P("   stale warnings: " + str(dqrows(edb, rid, "check_name like '%latest_order_not_stale'")))
    print("\n".join(out))


if __name__ == "__main__":
    main()
