"""Prints the tables needed for the screenshots (dq_results, pipeline_runs, payment reconciliation, capacity).

Run from the project folder, inside the Airflow container:   astro dev bash   then   python scripts/show_results.py
Optional: python scripts/show_results.py path/to/warehouse.duckdb
"""
import sys

import duckdb
import pandas as pd

pd.set_option("display.width", 250); pd.set_option("display.max_columns", 30); pd.set_option("display.max_colwidth", 40)
db = sys.argv[1] if len(sys.argv) > 1 else "include/warehouse_pravin.duckdb"
c = duckdb.connect(db, read_only=True)


def show(title, sql):
    print("\n" + "=" * 100 + f"\n{title}\n" + "=" * 100)
    try:
        print(c.execute(sql).df().to_string(index=False))
    except Exception as e:  # table not created yet
        print("(not available yet:", str(e).splitlines()[0], ")")


show("SCREENSHOT 5a - dq_results: latest run, summary per pipeline",
     "SELECT dag_id, run_id, status, count(*) AS checks FROM dq_results WHERE run_id IN "
     "(SELECT run_id FROM dq_results GROUP BY run_id ORDER BY max(checked_at) DESC LIMIT 2) GROUP BY ALL ORDER BY 1, 3")
show("SCREENSHOT 5b - dq_results: failing checks (first 15)",
     "SELECT dag_id, check_name, dimension, severity, rows_checked, rows_failed, status FROM dq_results "
     "WHERE status <> 'PASS' ORDER BY checked_at DESC, rows_failed DESC LIMIT 15")
show("SCREENSHOT 5c - pipeline_runs (run audit)", "SELECT * FROM pipeline_runs ORDER BY 1 DESC LIMIT 6")
show("SCREENSHOT 6a - payment reconciliation, ELT (summary by type)",
     "SELECT mismatch_type, count(*) AS orders FROM mart_payment_reconciliation GROUP BY 1 ORDER BY 2 DESC")
show("SCREENSHOT 6b - payment reconciliation, ETL (summary by type)",
     "SELECT mismatch_type, count(*) AS orders FROM etl_payment_reconciliation GROUP BY 1 ORDER BY 2 DESC")
show("SCREENSHOT 6c - payment reconciliation detail, ELT (first 12 rows)",
     "SELECT order_id, payment_id, mismatch_type, order_amount, amount_paid, unreconciled_amount, order_status, payment_status "
     "FROM mart_payment_reconciliation ORDER BY mismatch_type, order_id LIMIT 12")
show("SCREENSHOT 6d - total unreconciled amount (ELT vs ETL)",
     "SELECT 'ELT' AS pipeline, count(*) AS rows, round(sum(unreconciled_amount), 2) AS unreconciled_amount FROM mart_payment_reconciliation "
     "UNION ALL SELECT 'ETL', count(*), round(sum(unreconciled_amount), 2) FROM etl_payment_reconciliation")
show("BONUS - store capacity status (ELT)", "SELECT capacity_status, count(*) AS store_days FROM mart_store_capacity GROUP BY 1")
