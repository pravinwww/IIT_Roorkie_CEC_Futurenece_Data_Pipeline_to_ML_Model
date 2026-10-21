"""Shared settings, file contracts, masking and normalisation helpers.

Nothing in this module contains secrets. Tunable values are read, in this order, from
an environment variable (used by local tests) and then an Airflow Variable (used in the UI),
with safe defaults.
"""
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

USER = "pravin"
DAG_ETL = f"nammamart_etl_{USER}"
DAG_ELT = f"nammamart_elt_{USER}"

# ---- File contracts (schema the client promised us) -------------------------------------
ORDER_COLS = ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price",
              "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"]
FILES = {
    "orders": ("orders.csv", ORDER_COLS),
    "payments": ("payments.csv", ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid",
                                  "payment_status", "gateway_ref"]),
    "customers": ("customers.csv", ["customer_id", "full_name", "email", "phone", "city", "area", "pincode",
                                    "signup_date", "loyalty_tier", "is_active"]),
    "products": ("products.csv", ["product_id", "product_name", "category", "sub_category", "brand", "mrp",
                                  "cost_price", "unit", "is_perishable", "launch_date"]),
    "stores": ("stores.csv", ["store_id", "store_name", "zone", "area", "pincode", "open_date",
                              "manager_name", "capacity_orders_per_day"]),
}
INCREMENT_GLOB = "orders_*_increment.csv"

VALID_STATUS = ["DELIVERED", "CANCELLED", "RETURNED"]
VALID_MODES = ["UPI", "Card", "Cash on Delivery", "Wallet"]
VALID_PAY_STATUS = ["SUCCESS", "REFUNDED", "FAILED"]
VALID_ZONES = ["North", "South", "East", "West"]
MODE_MAP = {"upi": "UPI", "card": "Card", "cash on delivery": "Cash on Delivery", "cod": "Cash on Delivery",
            "wallet": "Wallet"}


class SchemaError(ValueError):
    """Raised when a source file does not match the agreed column contract (fail loudly)."""


@dataclass
class Settings:
    base: Path
    data_dir: Path
    db_path: Path
    quarantine_dir: Path
    work_dir: Path
    sql_dir: Path
    as_of: pd.Timestamp
    volume_check: bool


def _var(name: str, env: str, default: str) -> str:
    if os.environ.get(env):
        return os.environ[env]
    try:  # Airflow Variable (only available inside Airflow)
        from airflow.sdk import Variable  # Airflow 3
    except Exception:  # pragma: no cover
        try:
            from airflow.models import Variable  # Airflow 2
        except Exception:
            return default
    try:
        val = Variable.get(name, default=default)
    except TypeError:  # pragma: no cover  (older signature)
        val = Variable.get(name, default_var=default)
    except Exception:
        return default
    return default if val is None else str(val)


def get_settings() -> Settings:
    base = Path(os.environ.get("NM_BASE_DIR") or Path(__file__).resolve().parents[2])
    inc = base / "include"
    return Settings(
        base=base,
        data_dir=Path(_var("nammamart_data_dir", "NM_DATA_DIR", str(inc / "data" / "nammamart"))),
        db_path=Path(os.environ.get("NM_DB_PATH") or inc / f"warehouse_{USER}.duckdb"),
        quarantine_dir=Path(os.environ.get("NM_QUARANTINE_DIR") or inc / "quarantine" / USER),
        work_dir=Path(os.environ.get("NM_WORK_DIR") or inc / "work" / USER),
        sql_dir=inc / "sql",
        # The files are an export "as of" 1 Oct 2026. Using the wall clock would make the
        # "future date" checks depend on the day you run the DAG, so it is an explicit setting.
        as_of=pd.Timestamp(_var("nammamart_as_of", "NM_AS_OF", "2026-10-02 00:00:00")),
        volume_check=_var("nammamart_volume_check_enabled", "NM_VOLUME_CHECK", "false").lower() == "true",
    )


def safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(run_id))


def connect(db_path: Path, retries: int = 60, wait: float = 2.0):
    """Open the DuckDB warehouse. DuckDB allows one writing process at a time and both DAGs run at
    09:00, so wait and retry instead of failing on a lock."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    last = None
    for _ in range(retries):
        try:
            return duckdb.connect(str(db_path))
        except duckdb.IOException as exc:  # lock held by the other DAG
            last = exc
            time.sleep(wait)
    raise last


# ---- Reading source files ------------------------------------------------------------------
def read_raw(path: Path) -> pd.DataFrame:
    """Read a CSV with every value as text, exactly as received (blank stays blank)."""
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])


def check_schema(df: pd.DataFrame, expected: list[str], name: str) -> None:
    missing = [c for c in expected if c not in df.columns]
    extra = [c for c in df.columns if c not in expected]
    if missing or extra:
        raise SchemaError(f"{name}: schema changed. missing columns={missing} unexpected columns={extra}")


def find_increment_files(data_dir: Path) -> list[Path]:
    return sorted(data_dir.glob(INCREMENT_GLOB))


# ---- Normalisation -------------------------------------------------------------------------
def norm_mode(s: pd.Series) -> pd.Series:
    """'upi', 'UPI ' -> 'UPI'; unmappable values (e.g. 'Cashh') become NA."""
    return s.str.strip().str.lower().map(MODE_MAP)


def norm_status(s: pd.Series) -> pd.Series:
    return s.str.strip().str.upper()


def norm_tier(s: pd.Series) -> pd.Series:
    return s.str.strip().str.title()


def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.str.strip().replace("", pd.NA), errors="coerce")


def to_ts(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s.str.strip().replace("", pd.NA), format="ISO8601", errors="coerce")


# ---- PII masking ---------------------------------------------------------------------------
def mask_email(e) -> str:
    e = "" if e is None or (isinstance(e, float) and pd.isna(e)) else str(e).strip()
    if "@" not in e:
        return "***"
    user, dom = e.split("@", 1)
    return user[:2] + "***@" + dom


def mask_phone(p) -> str:
    d = re.sub(r"\D", "", "" if p is None or (isinstance(p, float) and pd.isna(p)) else str(p))
    if len(d) < 5:
        return "*" * len(d)
    return d[:2] + "*" * (len(d) - 4) + d[-2:]


def mask_name(n) -> str:
    parts = ("" if n is None or (isinstance(n, float) and pd.isna(n)) else str(n)).split()
    if not parts:
        return ""
    return parts[0] + (" " + parts[-1][0] + "." if len(parts) > 1 else "")


# ---- Per-task metrics (used by the run audit) ----------------------------------------------
def stats_dir(cfg: Settings, dag_id: str, run_id: str) -> Path:
    d = cfg.work_dir / dag_id / safe_run_id(run_id)
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_stats(cfg: Settings, dag_id: str, run_id: str, task: str, **metrics) -> None:
    metrics.update(task=task, finished_at=datetime.now().isoformat(timespec="seconds"))
    (stats_dir(cfg, dag_id, run_id) / f"stats_{task}.json").write_text(json.dumps(metrics, default=str))


def read_stats(cfg: Settings, dag_id: str, run_id: str) -> dict[str, dict]:
    out = {}
    for p in stats_dir(cfg, dag_id, run_id).glob("stats_*.json"):
        out[p.stem[6:]] = json.loads(p.read_text())
    return out


# ---- Warehouse objects shared by both pipelines -------------------------------------------
META_DDL = """
CREATE TABLE IF NOT EXISTS dq_results (
  run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR, severity VARCHAR,
  rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, checked_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP, rows_extracted BIGINT,
  rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR);
"""


def ensure_meta(con) -> None:
    con.execute(META_DDL)


def write_audit(cfg: Settings, dag_id: str, run_id: str, expected_tasks: list[str], start_time) -> dict:
    """Final task (trigger_rule=all_done): write exactly one row per run to pipeline_runs."""
    stats = read_stats(cfg, dag_id, run_id)
    missing = [t for t in expected_tasks if t not in stats]
    row = dict(
        run_id=run_id, dag_id=dag_id,
        start_time=pd.Timestamp(start_time) if start_time else None,
        end_time=pd.Timestamp.now(),
        rows_extracted=int(sum(s.get("rows_extracted", 0) for s in stats.values())),
        rows_loaded=int(sum(s.get("rows_loaded", 0) for s in stats.values())),
        rows_quarantined=int(sum(s.get("rows_quarantined", 0) for s in stats.values())),
        status="success" if not missing else "failed: " + ",".join(missing),
    )
    if row["start_time"] is None:
        times = [pd.Timestamp(s["finished_at"]) for s in stats.values() if "finished_at" in s]
        row["start_time"] = min(times) if times else row["end_time"]
    con = connect(cfg.db_path)
    try:
        ensure_meta(con)
        con.execute("DELETE FROM pipeline_runs WHERE run_id=? AND dag_id=?", [run_id, dag_id])
        con.execute("INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?)",
                    [row[k] for k in ["run_id", "dag_id", "start_time", "end_time", "rows_extracted",
                                      "rows_loaded", "rows_quarantined", "status"]])
    finally:
        con.close()
    return row
