"""ELT pipeline logic: Extract -> Load RAW (text) -> Transform in the warehouse with SQL files in include/sql/."""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from .common import (DAG_ELT, FILES, ORDER_COLS, Settings, check_schema, connect, ensure_meta,
                     find_increment_files, read_raw, write_audit, write_stats)

log = logging.getLogger(__name__)

MART_FILES = ["50_mart_daily_store_kpis.sql", "51_mart_category_margin.sql", "52_mart_delivery_sla.sql",
              "53_mart_payment_reconciliation.sql", "54_mart_top_customers.sql",
              "55_mart_payment_mode_revenue.sql", "56_mart_rejected_rows.sql",
              "57_mart_store_capacity.sql", "60_dim_customer_scd2.sql"]
ELT_TASKS = ["extract_orders", "extract_payments", "extract_customers", "extract_products", "extract_stores",
             "load_raw", "stg_customers", "stg_products", "stg_stores", "stg_orders", "load_raw_increment",
             "merge_stg_orders", "stg_payments", "dq_results"] + [f.split(".")[0].split("_", 1)[1] for f in MART_FILES]


def render(sql: str, cfg: Settings, run_id: str) -> str:
    return (sql.replace("${RUN_ID}", run_id.replace("'", "''")).replace("${DAG_ID}", DAG_ELT)
            .replace("${AS_OF}", str(cfg.as_of)).replace("${VOLUME_CHECK}", "1" if cfg.volume_check else "0"))


def run_sql_file(cfg: Settings, run_id: str, filename: str, con=None):
    sql = render((cfg.sql_dir / filename).read_text(), cfg, run_id)
    own = con is None
    con = con or connect(cfg.db_path)
    try:
        ensure_meta(con)
        con.execute((cfg.sql_dir / "01_raw_tables.sql").read_text())
        con.execute(sql)
    finally:
        if own:
            con.close()


def extract(cfg: Settings, run_id: str, name: str) -> dict:
    fname, cols = FILES[name]
    path = cfg.data_dir / fname
    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")
    head = read_raw(path)
    check_schema(head, cols, fname)  # fail loudly on a renamed/missing column
    write_stats(cfg, DAG_ELT, run_id, f"extract_{name}", rows_extracted=len(head))
    log.info("extract_%s: %d rows in %s", name, len(head), fname)
    return {"name": name, "path": str(path), "rows": len(head)}


def _append_raw(con, table: str, path: Path, fname: str, run_id: str, loaded_at: datetime) -> int:
    p = str(path).replace("'", "''")
    con.execute(f"""INSERT INTO {table} BY NAME
        SELECT *, TIMESTAMP '{loaded_at.isoformat(sep=' ')}' AS _loaded_at, '{fname}' AS _source_file,
               '{run_id.replace("'", "''")}' AS _run_id
        FROM read_csv('{p}', all_varchar = true, header = true, nullstr = '__no_nulls__')""")
    return con.execute(f"SELECT count(*) FROM {table} WHERE _run_id = ? AND _source_file = ?", [run_id, fname]).fetchone()[0]


def load_raw(cfg: Settings, run_id: str, extracts: dict) -> dict:
    """Append every row of every file exactly as received (all text). Raw is never cleaned or deleted."""
    con = connect(cfg.db_path)
    total = 0
    try:
        ensure_meta(con)
        con.execute((cfg.sql_dir / "01_raw_tables.sql").read_text())
        for name, ex in extracts.items():
            n = _append_raw(con, f"raw_{name}", Path(ex["path"]), FILES[name][0], run_id, datetime.now())
            total += n
            log.info("load_raw: raw_%s += %d rows", name, n)
    finally:
        con.close()
    write_stats(cfg, DAG_ELT, run_id, "load_raw", rows_loaded=total)
    return {"rows": total}


def load_raw_increment(cfg: Settings, run_id: str) -> dict:
    files = find_increment_files(cfg.data_dir)
    con = connect(cfg.db_path)
    total = 0
    try:
        con.execute((cfg.sql_dir / "01_raw_tables.sql").read_text())
        for f in files:
            check_schema(read_raw(f), ORDER_COLS, f.name)
            total += _append_raw(con, "raw_orders", f, f.name, run_id, datetime.now())
    finally:
        con.close()
    write_stats(cfg, DAG_ELT, run_id, "load_raw_increment", rows_extracted=total, rows_loaded=total)
    log.info("load_raw_increment: %d rows appended from %d file(s)", total, len(files))
    return {"files": [f.name for f in files], "rows": total}


def merge_stg_orders(cfg: Settings, run_id: str) -> dict:
    con = connect(cfg.db_path)
    try:
        run_sql_file(cfg, run_id, "20_view_orders.sql", con)
        n_inc, upd, ins = 0, 0, 0
        if con.execute("SELECT count(*) FROM raw_orders WHERE _source_file <> 'orders.csv'").fetchone()[0]:
            q = ("FROM v_orders_stage s WHERE s.src = 'orders_increment' AND s.rn = 1 AND s.failure_reason = ''")
            n_inc = con.execute(f"SELECT count(DISTINCT order_id) {q}").fetchone()[0]
            upd = con.execute(f"SELECT count(DISTINCT order_id) {q} AND s.order_id IN (SELECT order_id FROM stg_orders)").fetchone()[0]
            ins = n_inc - upd
            run_sql_file(cfg, run_id, "22_merge_orders_increment.sql", con)
        total = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
    finally:
        con.close()
    log.info("merge_stg_orders: increment rows applied=%d (updated=%d inserted=%d); stg_orders now %d rows",
             n_inc, upd, ins, total)
    write_stats(cfg, DAG_ELT, run_id, "merge_stg_orders", inserted=ins, updated=upd)
    return {"inserted": ins, "updated": upd, "stg_orders": total}


def dq_results(cfg: Settings, run_id: str) -> dict:
    con = connect(cfg.db_path)
    try:
        run_sql_file(cfg, run_id, "00_dq_catalog.sql", con)
        run_sql_file(cfg, run_id, "40_dq_results.sql", con)
        run_sql_file(cfg, run_id, "41_dq_volume.sql", con)
        quarantined = con.execute("SELECT count(*) FROM elt_quarantine WHERE run_id = ?", [run_id]).fetchone()[0]
        res = con.execute("SELECT status, count(*) FROM dq_results WHERE run_id=? AND dag_id=? GROUP BY 1",
                          [run_id, DAG_ELT]).fetchall()
    finally:
        con.close()
    log.info("dq_results: %s; %d rows in elt_quarantine", dict(res), quarantined)
    write_stats(cfg, DAG_ELT, run_id, "dq_results", rows_quarantined=quarantined)
    return {"quarantined": quarantined}


def mart(cfg: Settings, run_id: str, filename: str) -> dict:
    run_sql_file(cfg, run_id, filename)
    write_stats(cfg, DAG_ELT, run_id, filename.split(".")[0].split("_", 1)[1])
    return {"file": filename}


def run_all(cfg: Settings, run_id: str) -> dict:
    """Sequential run of every ELT task (what Airflow does, minus the parallel extract)."""
    ex = {n: extract(cfg, run_id, n) for n in FILES}
    load_raw(cfg, run_id, ex)
    for f in ["10_stg_customers.sql", "11_stg_products.sql", "12_stg_stores.sql"]:
        run_sql_file(cfg, run_id, f)
        write_stats(cfg, DAG_ELT, run_id, f"stg_{f.split('_', 2)[2].split('.')[0]}")
    run_sql_file(cfg, run_id, "20_view_orders.sql")
    run_sql_file(cfg, run_id, "21_stg_orders.sql")
    write_stats(cfg, DAG_ELT, run_id, "stg_orders")
    load_raw_increment(cfg, run_id)
    merged = merge_stg_orders(cfg, run_id)
    run_sql_file(cfg, run_id, "30_stg_payments.sql")
    write_stats(cfg, DAG_ELT, run_id, "stg_payments")
    dq_results(cfg, run_id)
    for f in MART_FILES:
        mart(cfg, run_id, f)
    audit = write_audit(cfg, DAG_ELT, run_id, ELT_TASKS, None)
    return {"elt_audit": audit, "elt_merge": merged}
