"""Data-quality rules for the ETL pipeline (pandas).

Every rule has a name, a dimension and a severity.
  Critical -> the row is quarantined (never reaches the warehouse reporting tables)
  Warning  -> the row loads, but the problem is logged in dq_results
The same rule names and logic are implemented in SQL for the ELT pipeline (include/sql/*.sql),
so both pipelines can be compared row for row.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .common import (VALID_MODES, VALID_PAY_STATUS, VALID_STATUS, VALID_ZONES, norm_mode, norm_status,
                     norm_tier, to_num, to_ts)

CRIT, WARN = "Critical", "Warning"
COMPLETENESS, UNIQUENESS, VALIDITY, ACCURACY, CONSISTENCY, TIMELINESS = (
    "Completeness", "Uniqueness", "Validity", "Accuracy", "Consistency", "Timeliness")


class Checker:
    """Collects the outcome of each check and the per-row reasons."""

    def __init__(self, df: pd.DataFrame, source: str, dag_id: str, run_id: str):
        self.df, self.source, self.dag_id, self.run_id = df, source, dag_id, run_id
        self.n = len(df)
        self.crit = [[] for _ in range(self.n)]
        self.warn = [[] for _ in range(self.n)]
        self.results: list[dict] = []
        self.now = pd.Timestamp.now()

    def add(self, name: str, dim: str, sev: str, failed) -> None:
        mask = np.asarray(pd.Series(failed).fillna(False), dtype=bool)
        self._record(name, dim, sev, self.n, int(mask.sum()))
        target = self.crit if sev == CRIT else self.warn
        for i in np.flatnonzero(mask):
            target[i].append(name)

    def add_scalar(self, name: str, dim: str, sev: str, failed: bool, checked: int = 1) -> None:
        self._record(name, dim, sev, checked, int(bool(failed)))

    def _record(self, name, dim, sev, checked, failed) -> None:
        status = "PASS" if failed == 0 else ("FAIL" if sev == CRIT else "WARN")
        self.results.append(dict(run_id=self.run_id, dag_id=self.dag_id, check_name=f"{self.source}.{name}",
                                 dimension=dim, severity=sev, rows_checked=checked, rows_failed=failed,
                                 status=status, checked_at=self.now))

    def annotated(self) -> pd.DataFrame:
        out = self.df.copy()
        out["failure_reason"] = ["; ".join(r) for r in self.crit]
        out["warning_reason"] = ["; ".join(r) for r in self.warn]
        out["dq_status"] = np.where(out["failure_reason"] != "", "QUARANTINE",
                                    np.where(out["warning_reason"] != "", "WARNING", "PASS"))
        return out


def _blank(s: pd.Series) -> pd.Series:
    return s.str.strip() == ""


def _dup(s: pd.Series) -> pd.Series:
    return s.duplicated(keep="first")


# ---------------------------------------------------------------- masters
def validate_customers(df, dag_id, run_id, as_of):
    ck = Checker(df, "customers", dag_id, run_id)
    ck.add("customer_id_not_null", COMPLETENESS, CRIT, _blank(df.customer_id))
    ck.add("customer_id_unique", UNIQUENESS, CRIT, _dup(df.customer_id))
    ck.add("email_valid", VALIDITY, WARN, ~df.email.str.strip().str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"))
    ck.add("phone_10_digits", VALIDITY, WARN, ~df.phone.str.strip().str.fullmatch(r"\d{10}"))
    ck.add("pincode_6_digits", VALIDITY, WARN, ~df.pincode.str.strip().str.fullmatch(r"\d{6}"))
    ck.add("city_not_null", COMPLETENESS, WARN, _blank(df.city))
    ck.add("loyalty_tier_casing", CONSISTENCY, WARN, df.loyalty_tier != norm_tier(df.loyalty_tier))
    ck.add("is_active_valid", VALIDITY, WARN, ~df.is_active.str.strip().isin(["Y", "N"]))
    sd = to_ts(df.signup_date)
    ck.add("signup_date_parses", VALIDITY, CRIT, sd.isna())
    ck.add("signup_date_not_future", TIMELINESS, CRIT, sd > as_of)
    return ck


def validate_products(df, dag_id, run_id, as_of):
    ck = Checker(df, "products", dag_id, run_id)
    mrp, cost = to_num(df.mrp), to_num(df.cost_price)
    ck.add("product_id_not_null", COMPLETENESS, CRIT, _blank(df.product_id))
    ck.add("product_id_unique", UNIQUENESS, CRIT, _dup(df.product_id))
    ck.add("category_not_null", COMPLETENESS, CRIT, _blank(df.category))
    ck.add("mrp_positive", ACCURACY, CRIT, ~(mrp > 0))
    ck.add("cost_price_positive", ACCURACY, CRIT, ~(cost > 0))
    ck.add("cost_not_above_mrp", ACCURACY, CRIT, cost > mrp)
    ck.add("launch_date_not_future", TIMELINESS, CRIT, to_ts(df.launch_date) > as_of)
    return ck


def validate_stores(df, dag_id, run_id, as_of):
    ck = Checker(df, "stores", dag_id, run_id)
    ck.add("store_id_not_null", COMPLETENESS, CRIT, _blank(df.store_id))
    ck.add("store_id_unique", UNIQUENESS, CRIT, _dup(df.store_id))
    ck.add("zone_valid", VALIDITY, WARN, ~df.zone.str.strip().isin(VALID_ZONES))
    ck.add("capacity_positive", ACCURACY, WARN, ~(to_num(df.capacity_orders_per_day) > 0))
    ck.add("pincode_6_digits", VALIDITY, WARN, ~df.pincode.str.strip().str.fullmatch(r"\d{6}"))
    return ck


# ---------------------------------------------------------------- facts
def validate_orders(df, source, dag_id, run_id, as_of, master_customers, clean_products, master_stores,
                    master_products=None):
    """df: raw text rows of one source file. master_*: id sets from the master files; clean_products: DataFrame(product_id, mrp)
    of the products that passed their own checks (used only for the unit-price warning)."""
    master_products = set(clean_products.product_id) if master_products is None else master_products
    ck = Checker(df, source, dag_id, run_id)
    q, u, d, a = (to_num(df[c]) for c in ["quantity", "unit_price", "discount_pct", "order_amount"])
    dm, ts = to_num(df.delivery_minutes), to_ts(df.order_ts)
    status, mode = norm_status(df.order_status), norm_mode(df.payment_mode)
    numeric_ok = q.notna() & u.notna() & d.notna() & a.notna()
    ck.add("customer_id_not_null", COMPLETENESS, CRIT, _blank(df.customer_id))
    ck.add("order_ts_not_null", COMPLETENESS, CRIT, _blank(df.order_ts))
    ck.add("delivered_has_delivery_minutes", COMPLETENESS, CRIT, (status == "DELIVERED") & dm.isna())
    ck.add("order_id_unique", UNIQUENESS, CRIT, _dup(df.order_id))
    ck.add("order_ts_parses", VALIDITY, CRIT, ts.isna() & ~_blank(df.order_ts))
    ck.add("order_status_valid", VALIDITY, CRIT, ~status.isin(VALID_STATUS))
    ck.add("order_status_casing", VALIDITY, WARN, status.isin(VALID_STATUS) & (df.order_status != status))
    ck.add("payment_mode_valid", VALIDITY, CRIT, mode.isna())
    ck.add("payment_mode_format", VALIDITY, WARN, mode.notna() & (df.payment_mode != mode))
    ck.add("numeric_fields_valid", VALIDITY, CRIT, ~numeric_ok)
    ck.add("discount_pct_range", VALIDITY, CRIT, ~((d >= 0) & (d <= 100)) & d.notna())
    ck.add("quantity_positive", ACCURACY, CRIT, q <= 0)
    calc = (q * u * (1 - d / 100)).round(2)
    ck.add("amount_matches_formula", ACCURACY, CRIT, numeric_ok & ((calc - a).abs() > 0.01))
    ck.add("delivery_minutes_plausible", ACCURACY, CRIT, (dm >= 120) | (dm <= 0))
    ck.add("customer_exists", CONSISTENCY, CRIT, ~_blank(df.customer_id) & ~df.customer_id.isin(master_customers))
    ck.add("product_exists", CONSISTENCY, CRIT, ~df.product_id.isin(master_products))
    ck.add("store_exists", CONSISTENCY, CRIT, ~df.store_id.isin(master_stores))
    mrp = df.product_id.map(clean_products.set_index("product_id")["mrp"])
    ck.add("unit_price_matches_mrp", CONSISTENCY, WARN, mrp.notna() & ((u - mrp).abs() > 0.005))
    ck.add("order_ts_not_future", TIMELINESS, CRIT, ts > as_of)
    latest = ts[ts <= as_of].max()   # newest order that is not future-dated
    ck.add_scalar("latest_order_not_stale", TIMELINESS, WARN,
                  bool(pd.isna(latest) or latest < as_of - pd.Timedelta(days=1)))
    return ck


def validate_payments(df, dag_id, run_id, as_of, order_amounts: pd.Series, order_modes: pd.Series):
    """order_amounts / order_modes: Series indexed by order_id built from the orders files."""
    ck = Checker(df, "payments", dag_id, run_id)
    amt, ts = to_num(df.amount_paid), to_ts(df.payment_ts)
    mode, status = norm_mode(df.payment_mode), df.payment_status.str.strip()
    exists = df.order_id.isin(order_amounts.index)
    oa = df.order_id.map(order_amounts)
    ck.add("payment_id_not_null", COMPLETENESS, CRIT, _blank(df.payment_id))
    ck.add("payment_id_unique", UNIQUENESS, CRIT, _dup(df.payment_id))
    ck.add("gateway_ref_present_for_online", COMPLETENESS, WARN, (mode != "Cash on Delivery") & _blank(df.gateway_ref))
    ck.add("payment_ts_parses", VALIDITY, CRIT, ts.isna())
    ck.add("payment_status_valid", VALIDITY, CRIT, ~status.isin(VALID_PAY_STATUS))
    ck.add("payment_mode_valid", VALIDITY, CRIT, mode.isna())
    ck.add("payment_mode_format", VALIDITY, WARN, mode.notna() & (df.payment_mode != mode))
    ck.add("amount_numeric", VALIDITY, CRIT, amt.isna())
    ck.add("amount_positive", ACCURACY, CRIT, amt <= 0)
    ck.add("payment_order_exists", CONSISTENCY, CRIT, ~exists)
    ck.add("success_amount_matches_order", CONSISTENCY, CRIT,
           exists & (status == "SUCCESS") & ((amt - oa).abs() > 0.01))
    om = df.order_id.map(order_modes)
    ck.add("payment_mode_matches_order", CONSISTENCY, WARN, exists & mode.notna() & om.notna() & (mode != om))
    ck.add("payment_ts_not_future", TIMELINESS, CRIT, ts > as_of)
    return ck
