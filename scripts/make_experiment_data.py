"""Create the modified COPIES of the dataset used by the Part F experiments. The originals are never touched.

  python scripts/make_experiment_data.py            # writes include/data/experiments/exp*/

Run an experiment by setting the Airflow Variable  nammamart_data_dir  to the folder, e.g.
  /usr/local/airflow/include/data/experiments/exp5_bad_rows
(Admin > Variables in the Airflow UI). Delete the Variable afterwards to go back to the real data.
"""
import shutil
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "include" / "data" / "nammamart"
OUT = ROOT / "include" / "data" / "experiments"
BASE = ["orders", "payments", "customers", "products", "stores"]


def fresh(name: str) -> Path:
    d = OUT / name
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    for b in BASE:
        shutil.copy(SRC / f"{b}.csv", d / f"{b}.csv")
    return d


def rd(name):
    return pd.read_csv(SRC / f"{name}.csv", dtype=str, keep_default_na=False)


def main():
    orders = rd("orders")
    # a clean, valid template row (the first DELIVERED order with a customer, delivery time and valid references)
    t = orders[(orders.order_status == "DELIVERED") & (orders.customer_id != "") & (orders.delivery_minutes != "")].iloc[0]

    # exp3: the incremental file is simply the real increment file added to the real folder (no copy needed)

    # exp5: five bad rows appended to a copy of orders.csv
    d = fresh("exp5_bad_rows")
    rows = []
    for i, (change, why) in enumerate([
            ({"customer_id": ""}, "null customer"), ({"quantity": "-2", "order_amount": "-2"}, "negative quantity"),
            ({"store_id": "S99"}, "unknown store"), ({"order_status": "SHIPPED"}, "status SHIPPED"),
            ({"order_ts": "2027-03-01 10:00:00"}, "2027 timestamp")], start=1):
        r = t.copy()
        r["order_id"] = f"ORDEXP{i:03d}"
        for k, v in change.items():
            r[k] = v
        rows.append(r)
    pd.concat([orders, pd.DataFrame(rows)]).to_csv(d / "orders.csv", index=False)

    # exp6: schema change - order_amount renamed to total
    d = fresh("exp6_schema_change")
    orders.rename(columns={"order_amount": "total"}).to_csv(d / "orders.csv", index=False)

    # exp7: volume anomaly - only 50 orders
    d = fresh("exp7_volume_50_orders")
    orders.head(50).to_csv(d / "orders.csv", index=False)

    # exp8: distribution drift - unit_price x100 for one store (S05); exp8b also scales order_amount (silent drift)
    for name, scale_amount in [("exp8_drift_unit_price", False), ("exp8b_drift_price_and_amount", True)]:
        d = fresh(name)
        o = orders.copy()
        m = o.store_id == "S05"
        o.loc[m, "unit_price"] = (pd.to_numeric(o.loc[m, "unit_price"]) * 100).astype(str)
        if scale_amount:
            o.loc[m, "order_amount"] = (pd.to_numeric(o.loc[m, "order_amount"], errors="coerce") * 100).astype(str)
        o.to_csv(d / "orders.csv", index=False)

    # exp9: freshness - the newest order is 30 days old
    d = fresh("exp9_stale_30_days")
    o = orders.copy()
    ts = pd.to_datetime(o.order_ts, errors="coerce")
    o["order_ts"] = (ts - pd.Timedelta(days=30)).dt.strftime("%Y-%m-%d %H:%M:%S").where(ts.notna(), o.order_ts)
    o.to_csv(d / "orders.csv", index=False)

    # exp4: missing file - a folder without products.csv (or simply rename products.csv in the real folder)
    d = fresh("exp4_missing_products")
    (d / "products.csv").unlink()
    print("created:", ", ".join(sorted(p.name for p in OUT.iterdir())))


if __name__ == "__main__":
    main()
