"""ETL pipeline logic: Extract -> Validate -> (Quarantine) -> Transform -> Load -> Reconcile -> Report.

Every public function is one Airflow task (see dags/nammamart_etl_pravin.py) and can also be run
without Airflow (scripts/run_local.py) for testing.
"""
from __future__ import annotations

import logging
import pickle
from pathlib import Path

import pandas as pd

from . import dq
from .common import (DAG_ETL, FILES, ORDER_COLS, Settings, check_schema, connect, ensure_meta,
                     find_increment_files, mask_email, mask_name, mask_phone, norm_mode, norm_status,
                     norm_tier, read_raw, safe_run_id, to_num, to_ts, write_stats)

log = logging.getLogger(__name__)


def _wd(cfg: Settings, run_id: str) -> Path:
    d = cfg.work_dir / "etl" / safe_run_id(run_id)
    d.mkdir(parents=True, exist_ok=True)
    return d


def _save(obj, path: Path) -> str:
    with open(path, "wb") as f:
        pickle.dump(obj, f)
    return str(path)


def _load(path: str):
    with open(path, "rb") as f:
        return pickle.load(f)


# =============================================================== 1. EXTRACT (5 parallel tasks)
def extract(cfg: Settings, run_id: str, name: str) -> dict:
    fname, cols = FILES[name]
    path = cfg.data_dir / fname
    if not path.exists():  # fail loudly -> Airflow retries -> up_for_retry -> failed
        raise FileNotFoundError(f"Source file not found: {path}")
    df = read_raw(path)
    check_schema(df, cols, fname)
    df["_source_file"] = fname
    if name == "orders":  # next-day files are picked up automatically when present
        parts = [df]
        for inc in find_increment_files(cfg.data_dir):
            d = read_raw(inc)
            check_schema(d, ORDER_COLS, inc.name)
            d["_source_file"] = inc.name
            parts.append(d)
        df = pd.concat(parts, ignore_index=True)
    out = _save(df, _wd(cfg, run_id) / f"extract_{name}.pkl")
    write_stats(cfg, DAG_ETL, run_id, f"extract_{name}", rows_extracted=len(df))
    log.info("extract_%s: %d rows from %s", name, len(df), fname)
    return {"name": name, "path": out, "rows": len(df)}


# =============================================================== 2. VALIDATE
def _replace_dq(con, results: list[dict], run_id: str) -> None:
    ensure_meta(con)
    con.execute("DELETE FROM dq_results WHERE run_id=? AND dag_id=?", [run_id, DAG_ETL])
    if results:
        df = pd.DataFrame(results)  # noqa: F841
        con.execute("INSERT INTO dq_results SELECT run_id, dag_id, check_name, dimension, severity, rows_checked, "
                    "rows_failed, status, checked_at FROM df")


def validate(cfg: Settings, run_id: str, extracts: dict[str, dict]) -> dict:
    d = {k: _load(v["path"]) for k, v in extracts.items()}
    results, summary, passed, failed = [], {}, {}, {}

    def finish(label, ck):
        ann = ck.annotated()
        passed[label] = ann[ann.dq_status != "QUARANTINE"].reset_index(drop=True)
        failed[label] = ann[ann.dq_status == "QUARANTINE"].reset_index(drop=True)
        results.extend(ck.results)
        summary[label] = dict(rows=len(ann), passed=len(passed[label]), failed=len(failed[label]),
                              warnings=int((ann.dq_status == "WARNING").sum()))
        log.info("validate %-17s rows=%d passed=%d quarantined=%d warnings=%d", label, len(ann),
                 len(passed[label]), len(failed[label]), summary[label]["warnings"])

    # masters first: orders are checked against the CLEAN masters
    finish("customers", dq.validate_customers(d["customers"], DAG_ETL, run_id, cfg.as_of))
    finish("products", dq.validate_products(d["products"], DAG_ETL, run_id, cfg.as_of))
    finish("stores", dq.validate_stores(d["stores"], DAG_ETL, run_id, cfg.as_of))
    # Consistency: an order must reference an id that exists in the master FILE. (A master row that fails its own
    # critical check is quarantined from the dimension; orders that point at it still load, and the dimension gets
    # an 'UNKNOWN' inferred member so the star schema has no orphan keys.)
    clean_cust = set(d["customers"].customer_id.str.strip())
    clean_prod = passed["products"][["product_id", "mrp"]].assign(mrp=lambda x: to_num(x.mrp))
    master_prod = set(d["products"].product_id.str.strip())
    clean_store = set(d["stores"].store_id.str.strip())

    orders = d["orders"]
    base = orders[orders._source_file == FILES["orders"][0]].reset_index(drop=True)
    inc = orders[orders._source_file != FILES["orders"][0]].reset_index(drop=True)
    finish("orders", dq.validate_orders(base, "orders", DAG_ETL, run_id, cfg.as_of, clean_cust, clean_prod, clean_store, master_prod))
    if len(inc):
        finish("orders_increment", dq.validate_orders(inc, "orders_increment", DAG_ETL, run_id, cfg.as_of,
                                                      clean_cust, clean_prod, clean_store, master_prod))
    # order amounts/modes as received (increment rows override base rows) -> used to check payments
    latest = pd.concat([base.drop_duplicates("order_id", keep="first"),
                        inc.sort_values("_source_file").drop_duplicates("order_id", keep="last")]) \
        .drop_duplicates("order_id", keep="last").set_index("order_id")
    finish("payments", dq.validate_payments(d["payments"], DAG_ETL, run_id, cfg.as_of,
                                            to_num(latest.order_amount), norm_mode(latest.payment_mode)))

    # Volume check: rows per file are recorded on every run (history for the comparison). The check only FLAGS a
    # problem when the Airflow Variable nammamart_volume_check_enabled = true: a file with < 10% of the largest row count
    # seen in the last 5 runs raises a Warning.
    con = connect(cfg.db_path)
    try:
        ensure_meta(con)
        for label in ["orders", "payments", "customers", "products", "stores"]:
            name = f"{label}.volume_vs_recent_max"
            rows = summary[label]["rows"]
            prev = con.execute("SELECT max(rows_checked) FROM (SELECT rows_checked FROM dq_results WHERE dag_id=? "
                               "AND check_name=? AND run_id<>? ORDER BY checked_at DESC LIMIT 5)",
                               [DAG_ETL, name, run_id]).fetchone()[0]
            flag = int(bool(cfg.volume_check and prev and rows < 0.1 * prev))
            results.append(dict(run_id=run_id, dag_id=DAG_ETL, check_name=name, dimension=dq.COMPLETENESS,
                                severity=dq.WARN, rows_checked=rows, rows_failed=flag,
                                status="WARN" if flag else "PASS", checked_at=pd.Timestamp.now()))
            if flag:
                log.warning("volume check: %s has %d rows, recent max was %d", label, rows, prev)
    finally:
        con.close()

    wd = _wd(cfg, run_id)
    con = connect(cfg.db_path)
    try:
        _replace_dq(con, results, run_id)
    finally:
        con.close()
    out = {"passed": _save(passed, wd / "passed.pkl"), "failed": _save(failed, wd / "failed.pkl"),
           "summary": summary, "rows_quarantined": sum(v["failed"] for v in summary.values())}
    write_stats(cfg, DAG_ETL, run_id, "validate", rows_quarantined=0, checks=len(results))
    return out


# =============================================================== 3. QUARANTINE
def quarantine(cfg: Settings, run_id: str, validated: dict) -> dict:
    failed = _load(validated["failed"])
    cfg.quarantine_dir.mkdir(parents=True, exist_ok=True)
    rows, files = [], []
    for label, df in failed.items():
        if df.empty:
            continue
        out = df.drop(columns=["warning_reason", "dq_status"]).copy()
        if label == "customers":  # quarantine files must never carry readable personal data
            out["full_name"] = out.full_name.map(mask_name)
            out["email"] = out.email.map(mask_email)
            out["phone"] = out.phone.map(mask_phone)
        out["run_id"] = run_id
        p = cfg.quarantine_dir / f"{label}_failed_{safe_run_id(run_id)}.csv"
        out.to_csv(p, index=False)
        files.append(str(p))
        ex = df.assign(reason=df.failure_reason.str.split("; ")).explode("reason")
        for reason, n in ex.groupby("reason").size().items():
            rows.append((run_id, label, reason, int(n)))
    con = connect(cfg.db_path)
    try:
        con.execute("CREATE TABLE IF NOT EXISTS etl_rejected_rows (run_id VARCHAR, source_file VARCHAR, "
                    "failure_reason VARCHAR, rows_rejected BIGINT, rejected_at TIMESTAMP)")
        con.execute("DELETE FROM etl_rejected_rows WHERE run_id=?", [run_id])
        for r in rows:
            con.execute("INSERT INTO etl_rejected_rows VALUES (?,?,?,?,now())", list(r))
    finally:
        con.close()
    n = sum(len(v) for v in failed.values())
    write_stats(cfg, DAG_ETL, run_id, "quarantine", rows_quarantined=n)
    log.info("quarantine: %d rows written to %d files in %s", n, len(files), cfg.quarantine_dir)
    return {"files": files, "rows": n}


# =============================================================== 4. TRANSFORM
def transform(cfg: Settings, run_id: str, validated: dict) -> dict:
    passed, failed = _load(validated["passed"]), _load(validated["failed"])
    ts_now = pd.Timestamp.now()

    c = passed["customers"].copy()
    dim_customer = pd.DataFrame({
        "customer_id": c.customer_id.str.strip(), "customer_name_masked": c.full_name.map(mask_name),
        "email_masked": c.email.map(mask_email), "phone_masked": c.phone.map(mask_phone),
        "city": c.city.str.strip(), "area": c.area.str.strip(), "pincode": c.pincode.str.strip(),
        "signup_date": to_ts(c.signup_date).dt.date, "loyalty_tier": norm_tier(c.loyalty_tier),
        "is_active": c.is_active.str.strip()})
    p = passed["products"].copy()
    dim_product = pd.DataFrame({
        "product_id": p.product_id.str.strip(), "product_name": p.product_name.str.strip(),
        "category": p.category.str.strip(), "sub_category": p.sub_category.str.strip(),
        "brand": p.brand.str.strip(), "mrp": to_num(p.mrp), "cost_price": to_num(p.cost_price),
        "unit": p.unit.str.strip(), "is_perishable": p.is_perishable.str.strip().str.lower() == "true",
        "launch_date": to_ts(p.launch_date).dt.date})
    s = passed["stores"].copy()
    zone = s.zone.str.strip()
    dim_store = pd.DataFrame({
        "store_id": s.store_id.str.strip(), "store_name": s.store_name.str.strip(),
        "zone": zone.where(zone.isin(dq.VALID_ZONES), "UNASSIGNED"), "area": s.area.str.strip(),
        "pincode": s.pincode.str.strip(), "open_date": to_ts(s.open_date).dt.date,
        "manager_name": s.manager_name.str.strip(), "capacity_orders_per_day": to_num(s.capacity_orders_per_day)})

    # orders: passed base rows, then passed increment rows overwrite by order_id (upsert semantics)
    parts = [passed["orders"]] + ([passed["orders_increment"]] if "orders_increment" in passed else [])
    o = pd.concat(parts, ignore_index=True)
    o = o.sort_values("_source_file", kind="stable")  # orders.csv < orders_2026-..._increment.csv
    o = o.drop_duplicates("order_id", keep="last").reset_index(drop=True)
    q, u, dsc, amt = (to_num(o[x]) for x in ["quantity", "unit_price", "discount_pct", "order_amount"])
    ts, dm = to_ts(o.order_ts), to_num(o.delivery_minutes)
    status = norm_status(o.order_status)
    cost = o.product_id.map(dim_product.set_index("product_id").cost_price)
    delivered = status == "DELIVERED"
    fact = pd.DataFrame({
        "order_id": o.order_id.str.strip(), "order_ts": ts, "order_date": ts.dt.date,
        "customer_id": o.customer_id.str.strip(), "store_id": o.store_id.str.strip(),
        "product_id": o.product_id.str.strip(), "quantity": q.astype("int64"), "unit_price": u,
        "discount_pct": dsc, "order_amount": amt, "payment_mode": norm_mode(o.payment_mode),
        "order_status": status, "channel": o.channel.str.strip(), "delivery_minutes": dm,
        "net_revenue": amt.where(delivered, 0.0).round(2),
        "gross_margin": (amt - q * cost).where(delivered, 0.0).round(2),
        "within_15_min": (dm <= 15).where(delivered).astype("boolean"),
        "source_file": o._source_file, "_run_id": run_id, "_loaded_at": ts_now})
    # inferred dimension members: keys used by good orders whose master row was quarantined
    miss_c = sorted(set(fact.customer_id) - set(dim_customer.customer_id))
    if miss_c:
        dim_customer = pd.concat([dim_customer, pd.DataFrame({
            "customer_id": miss_c, "customer_name_masked": "UNKNOWN", "email_masked": "***", "phone_masked": "***",
            "city": None, "area": None, "pincode": None, "signup_date": None, "loyalty_tier": "UNKNOWN",
            "is_active": None})], ignore_index=True)
    miss_p = sorted(set(fact.product_id) - set(dim_product.product_id))
    if miss_p:
        dim_product = pd.concat([dim_product, pd.DataFrame({
            "product_id": miss_p, "product_name": "UNKNOWN", "category": "UNKNOWN", "sub_category": None, "brand": None,
            "mrp": None, "cost_price": None, "unit": None, "is_perishable": None, "launch_date": None})], ignore_index=True)
    miss_s = sorted(set(fact.store_id) - set(dim_store.store_id))
    if miss_s:
        dim_store = pd.concat([dim_store, pd.DataFrame({
            "store_id": miss_s, "store_name": "UNKNOWN", "zone": "UNASSIGNED", "area": None, "pincode": None,
            "open_date": None, "manager_name": None, "capacity_orders_per_day": None})], ignore_index=True)
    if miss_c or miss_p or miss_s:
        log.warning("inferred dimension members added: customers=%d products=%d stores=%d (master rows quarantined)",
                    len(miss_c), len(miss_p), len(miss_s))
    wd = _wd(cfg, run_id)
    # order ids rejected this run that have no good version -> remove stale copy from the fact table
    good_ids = set(fact.order_id)
    bad_ids = set()
    for k in ("orders", "orders_increment"):
        if k in failed:
            bad_ids |= set(failed[k].order_id)
    out = {"dim_customer": dim_customer, "dim_product": dim_product, "dim_store": dim_store, "fact": fact,
           "stale_ids": sorted(bad_ids - good_ids)}
    path = _save(out, wd / "transformed.pkl")
    write_stats(cfg, DAG_ETL, run_id, "transform", rows_transformed=len(fact))
    log.info("transform: fact=%d customers=%d products=%d stores=%d", len(fact), len(dim_customer),
             len(dim_product), len(dim_store))
    return {"path": path, "fact_rows": len(fact)}


# =============================================================== 5. LOAD (idempotent star schema)
DDL = """
CREATE TABLE IF NOT EXISTS etl_dim_customer (customer_id VARCHAR PRIMARY KEY, customer_name_masked VARCHAR,
  email_masked VARCHAR, phone_masked VARCHAR, city VARCHAR, area VARCHAR, pincode VARCHAR, signup_date DATE,
  loyalty_tier VARCHAR, is_active VARCHAR);
CREATE TABLE IF NOT EXISTS etl_dim_product (product_id VARCHAR PRIMARY KEY, product_name VARCHAR, category VARCHAR,
  sub_category VARCHAR, brand VARCHAR, mrp DOUBLE, cost_price DOUBLE, unit VARCHAR, is_perishable BOOLEAN,
  launch_date DATE);
CREATE TABLE IF NOT EXISTS etl_dim_store (store_id VARCHAR PRIMARY KEY, store_name VARCHAR, zone VARCHAR,
  area VARCHAR, pincode VARCHAR, open_date DATE, manager_name VARCHAR, capacity_orders_per_day DOUBLE);
CREATE TABLE IF NOT EXISTS etl_fact_orders (order_id VARCHAR PRIMARY KEY, order_ts TIMESTAMP, order_date DATE,
  customer_id VARCHAR, store_id VARCHAR, product_id VARCHAR, quantity BIGINT, unit_price DOUBLE,
  discount_pct DOUBLE, order_amount DOUBLE, payment_mode VARCHAR, order_status VARCHAR, channel VARCHAR,
  delivery_minutes DOUBLE, net_revenue DOUBLE, gross_margin DOUBLE, within_15_min BOOLEAN,
  source_file VARCHAR, _run_id VARCHAR, _loaded_at TIMESTAMP);
"""


def _upsert(con, table: str, key: str, df: pd.DataFrame) -> None:
    con.register("incoming", df)
    con.execute(f"DELETE FROM {table} WHERE {key} IN (SELECT {key} FROM incoming)")
    cols = ", ".join(df.columns)
    con.execute(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM incoming")
    con.unregister("incoming")


def _scd2_customers(cfg: Settings, con) -> None:
    """Bonus: SCD Type 2 customer history. Same SQL file as the ELT pipeline, pointed at the ETL tables."""
    sql = (cfg.sql_dir / "60_dim_customer_scd2.sql").read_text()
    sql = (sql.replace("dim_customer_scd2", "etl_dim_customer_scd2")
              .replace("FROM stg_customers WHERE dq_status <> 'QUARANTINE'",
                       "FROM etl_dim_customer WHERE loyalty_tier <> 'UNKNOWN'"))
    con.execute(sql)


def load(cfg: Settings, run_id: str, transformed: dict) -> dict:
    t = _load(transformed["path"])
    fact = t["fact"]
    con = connect(cfg.db_path)
    try:
        con.execute(DDL)
        ensure_meta(con)
        con.execute("BEGIN")
        old = con.execute("SELECT order_id, order_status, order_amount, delivery_minutes, quantity "
                          "FROM etl_fact_orders").df().set_index("order_id")
        new = fact.set_index("order_id")
        both = new.index.intersection(old.index)
        changed = 0
        if len(both):
            a, b = new.loc[both], old.loc[both]
            diff = ((a.order_status != b.order_status) | ((a.order_amount - b.order_amount).abs() > 0.001) |
                    (a.quantity != b.quantity) | (a.delivery_minutes.fillna(-1) != b.delivery_minutes.fillna(-1)))
            changed = int(diff.sum())
        inserted, updated = len(new.index.difference(old.index)), changed
        unchanged = len(both) - changed
        _upsert(con, "etl_dim_customer", "customer_id", t["dim_customer"])
        _upsert(con, "etl_dim_product", "product_id", t["dim_product"])
        _upsert(con, "etl_dim_store", "store_id", t["dim_store"])
        if t["stale_ids"]:  # an order that is now rejected must not stay in the finance table
            con.register("stale", pd.DataFrame({"order_id": t["stale_ids"]}))
            con.execute("DELETE FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM stale)")
            con.unregister("stale")
        _upsert(con, "etl_fact_orders", "order_id", fact)
        _scd2_customers(cfg, con)
        con.execute("COMMIT")
        total = con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0]
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    loaded = len(fact) + len(t["dim_customer"]) + len(t["dim_product"]) + len(t["dim_store"])
    write_stats(cfg, DAG_ETL, run_id, "load", rows_loaded=loaded, inserted=inserted, updated=updated,
                unchanged=unchanged)
    log.info("load: fact inserted=%d updated=%d unchanged=%d | etl_fact_orders now has %d rows",
             inserted, updated, unchanged, total)
    return {"inserted": inserted, "updated": updated, "unchanged": unchanged, "fact_total": total}


# =============================================================== 6. RECONCILE
def reconcile(cfg: Settings, run_id: str, extracts: dict, loaded: dict) -> dict:
    pay = _load(extracts["payments"]["path"]).drop_duplicates("payment_id", keep="first").copy()
    raw_orders = _load(extracts["orders"]["path"])
    raw_ids = set(raw_orders.order_id)
    pay["amt"] = to_num(pay.amount_paid)
    pay["pstatus"] = pay.payment_status.str.strip()
    con = connect(cfg.db_path)
    try:
        fact = con.execute("SELECT order_id, order_status, order_amount FROM etl_fact_orders").df()
        rows = []
        fo = fact.set_index("order_id")
        by_order = pay.groupby("order_id")
        for oid, r in fo.iterrows():
            grp = by_order.get_group(oid) if oid in by_order.groups else None
            if r.order_status == "CANCELLED":
                if grp is not None:
                    p = grp.iloc[0]
                    rows.append((oid, p.payment_id, "PAYMENT_FOR_CANCELLED", r.order_amount, p.amt, p.amt,
                                 r.order_status, p.pstatus, "Cancelled order has a payment"))
                continue
            if grp is None:
                rows.append((oid, None, "NO_PAYMENT", r.order_amount, None, r.order_amount, r.order_status, None,
                             "No payment row in the gateway export"))
                continue
            p = grp.iloc[0]
            if r.order_status == "DELIVERED" and p.pstatus == "FAILED":
                rows.append((oid, p.payment_id, "PAYMENT_FAILED", r.order_amount, p.amt, r.order_amount,
                             r.order_status, p.pstatus, "Order delivered but the gateway payment FAILED"))
            elif p.pstatus in ("SUCCESS", "REFUNDED") and abs(p.amt - r.order_amount) > 0.01:
                rows.append((oid, p.payment_id, "AMOUNT_MISMATCH", r.order_amount, p.amt,
                             round(abs(p.amt - r.order_amount), 2), r.order_status, p.pstatus,
                             "Amount paid differs from order_amount"))
        clean_ids = set(fo.index)
        for _, p in pay[~pay.order_id.isin(clean_ids) & pay.pstatus.isin(["SUCCESS", "REFUNDED"])].iterrows():
            why = ("order_id not present in orders files" if p.order_id not in raw_ids
                   else "order rejected by quality checks (in quarantine)")
            rows.append((p.order_id, p.payment_id, "PAYMENT_WITHOUT_CLEAN_ORDER", None, p.amt, p.amt, None,
                         p.pstatus, "Money received but no clean order: " + why))
        cols = ["order_id", "payment_id", "mismatch_type", "order_amount", "amount_paid", "unreconciled_amount",
                "order_status", "payment_status", "reason"]
        df = pd.DataFrame(rows, columns=cols)
        df.insert(0, "run_id", run_id)
        df["reconciled_at"] = pd.Timestamp.now()
        con.execute("DROP TABLE IF EXISTS etl_payment_reconciliation")
        con.execute("CREATE TABLE etl_payment_reconciliation AS SELECT * FROM df")
    finally:
        con.close()
    summ = df.groupby("mismatch_type").agg(rows=("order_id", "size"), amount=("unreconciled_amount", "sum"))
    log.info("reconcile: %d mismatches, total unreconciled amount %.2f\n%s", len(df),
             df.unreconciled_amount.sum(), summ.to_string())
    write_stats(cfg, DAG_ETL, run_id, "reconcile", mismatches=len(df))
    return {"mismatches": len(df), "unreconciled_amount": float(df.unreconciled_amount.sum())}


# =============================================================== 7. REPORT
def report(cfg: Settings, run_id: str) -> dict:
    con = connect(cfg.db_path)
    try:
        con.execute("""CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT f.order_date, s.zone, f.store_id, s.store_name, count(*) AS delivered_orders,
                   round(sum(f.net_revenue), 2) AS net_revenue
            FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
            WHERE f.order_status = 'DELIVERED' GROUP BY ALL ORDER BY 1, 2, 3""")
        con.execute("""CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT p.category, round(sum(f.net_revenue), 2) AS net_revenue,
                   round(sum(f.net_revenue - f.quantity * p.cost_price), 2) AS gross_margin,
                   round(100 * sum(f.net_revenue - f.quantity * p.cost_price) / sum(f.net_revenue), 2) AS margin_pct
            FROM etl_fact_orders f JOIN etl_dim_product p USING (product_id)
            WHERE f.order_status = 'DELIVERED' GROUP BY 1 ORDER BY 1""")
        con.execute("""CREATE OR REPLACE TABLE etl_store_capacity AS
            SELECT f.order_date, f.store_id, s.store_name, s.capacity_orders_per_day AS capacity,
                   count(*) AS orders_that_day,
                   round(100.0 * count(*) / nullif(s.capacity_orders_per_day, 0), 2) AS utilisation_pct,
                   CASE WHEN (s.capacity_orders_per_day IS NULL OR s.capacity_orders_per_day <= 0) THEN 'CAPACITY_UNKNOWN'
                        WHEN count(*) > s.capacity_orders_per_day THEN 'OVER_CAPACITY'
                        WHEN count(*) > 0.8 * s.capacity_orders_per_day THEN 'NEAR_CAPACITY' ELSE 'OK' END AS capacity_status
            FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id) GROUP BY 1, 2, 3, 4 ORDER BY 1, 2""")
        cap = con.execute("SELECT capacity_status, count(*) AS store_days FROM etl_store_capacity GROUP BY 1 ORDER BY 1").df()
        log.info("etl_store_capacity:\n%s", cap.to_string(index=False))
        zone = con.execute("SELECT zone, round(sum(net_revenue),2) AS net_revenue, sum(delivered_orders) AS orders "
                           "FROM etl_daily_zone_revenue GROUP BY 1 ORDER BY 1").df()
        cat = con.execute("SELECT * FROM etl_category_margin").df()
        n = con.execute("SELECT count(*) FROM etl_daily_zone_revenue").fetchone()[0]
    finally:
        con.close()
    log.info("etl_daily_zone_revenue (%d rows) - totals by zone:\n%s", n, zone.to_string(index=False))
    log.info("etl_category_margin:\n%s", cat.to_string(index=False))
    print(zone.to_string(index=False)), print(cat.to_string(index=False))
    write_stats(cfg, DAG_ETL, run_id, "report")
    return {"zone_rows": n}


ETL_TASKS = ["extract_orders", "extract_payments", "extract_customers", "extract_products", "extract_stores",
             "validate", "quarantine", "transform", "load", "reconcile", "report"]
