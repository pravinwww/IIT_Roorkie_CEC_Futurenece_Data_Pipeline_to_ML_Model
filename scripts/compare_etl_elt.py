"""Check that the ETL and ELT pipelines agree (same warehouse file). Exit code 1 if they do not."""
import sys

import duckdb

db = sys.argv[1]
con = duckdb.connect(db, read_only=True)
q = lambda s: con.execute(s).fetchall()  # noqa: E731
bad = 0


def check(label, a, b):
    global bad
    ok = a == b
    bad += not ok
    print(("OK   " if ok else "DIFF ") + label, "" if ok else f"\n   ETL={str(a)[:300]}\n   ELT={str(b)[:300]}")


check("clean orders (row count)", q("SELECT count(*) FROM etl_fact_orders"),
      q("SELECT count(*) FROM stg_orders WHERE dq_status <> 'QUARANTINE'"))
check("clean order ids", q("SELECT order_id, order_status, order_amount, net_revenue, gross_margin FROM etl_fact_orders ORDER BY 1"),
      q("SELECT order_id, order_status, order_amount, net_revenue, gross_margin FROM stg_orders WHERE dq_status <> 'QUARANTINE' ORDER BY 1"))
check("total net revenue", q("SELECT round(sum(net_revenue),2) FROM etl_fact_orders"),
      q("SELECT round(sum(net_revenue),2) FROM stg_orders WHERE dq_status <> 'QUARANTINE'"))
check("rejected rows by file+reason (latest run each)",
      q("SELECT source_file, failure_reason, rows_rejected FROM etl_rejected_rows WHERE run_id=(SELECT run_id FROM pipeline_runs WHERE dag_id='nammamart_etl_pravin' ORDER BY start_time DESC LIMIT 1) ORDER BY 1,2"),
      q("SELECT source_file, failure_reason, rows_rejected FROM mart_rejected_rows WHERE run_id=(SELECT run_id FROM pipeline_runs WHERE dag_id='nammamart_elt_pravin' ORDER BY start_time DESC LIMIT 1) ORDER BY 1,2"))
for t, a, b in [("customers", "etl_dim_customer", "stg_customers"), ("products", "etl_dim_product", "stg_products"),
                ("stores", "etl_dim_store", "stg_stores")]:
    k = {"customers": "customer_id", "products": "product_id", "stores": "store_id"}[t]
    unk = {"customers": "customer_name_masked", "products": "product_name", "stores": "store_name"}[t]
    check(f"clean {t} ids (inferred UNKNOWN members excluded)", q(f"SELECT {k} FROM {a} WHERE {unk} <> 'UNKNOWN' ORDER BY 1"), q(f"SELECT {k} FROM {b} WHERE dq_status<>'QUARANTINE' ORDER BY 1"))
check("masked customers", q("SELECT customer_id, customer_name_masked, email_masked, phone_masked FROM etl_dim_customer WHERE customer_name_masked <> 'UNKNOWN' ORDER BY 1"),
      q("SELECT customer_id, customer_name_masked, email_masked, phone_masked FROM stg_customers WHERE dq_status<>'QUARANTINE' ORDER BY 1"))
check("dq_results failed rows per check",
      q("SELECT check_name, rows_checked, rows_failed FROM dq_results WHERE dag_id='nammamart_etl_pravin' AND run_id=(SELECT run_id FROM pipeline_runs WHERE dag_id='nammamart_etl_pravin' ORDER BY start_time DESC LIMIT 1) ORDER BY 1"),
      q("SELECT check_name, rows_checked, rows_failed FROM dq_results WHERE dag_id='nammamart_elt_pravin' AND run_id=(SELECT run_id FROM pipeline_runs WHERE dag_id='nammamart_elt_pravin' ORDER BY start_time DESC LIMIT 1) ORDER BY 1"))
check("category margin", q("SELECT * FROM etl_category_margin ORDER BY 1"), q("SELECT * FROM mart_category_margin ORDER BY 1"))
check("daily zone/store revenue", q("SELECT order_date, zone, store_id, net_revenue FROM etl_daily_zone_revenue ORDER BY 1,2,3"),
      q("SELECT order_date, zone, store_id, net_revenue FROM mart_daily_store_kpis WHERE delivered_orders>0 ORDER BY 1,2,3"))
check("reconciliation (type, order, amount)", q("SELECT mismatch_type, order_id, payment_id, round(unreconciled_amount,2) FROM etl_payment_reconciliation ORDER BY 1,2,3"),
      q("SELECT mismatch_type, order_id, payment_id, round(unreconciled_amount,2) FROM mart_payment_reconciliation ORDER BY 1,2,3"))
check("top 10 customers", q("""SELECT f.customer_id, c.customer_name_masked, c.loyalty_tier, round(sum(f.net_revenue),2) s, count(*)
      FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id) WHERE f.order_status='DELIVERED'
      GROUP BY ALL ORDER BY s DESC, f.customer_id LIMIT 10"""),
      q("SELECT customer_id, customer_name_masked, loyalty_tier, total_spend, delivered_orders FROM mart_top_customers ORDER BY rank"))
check("revenue by payment mode", q("SELECT payment_mode, round(sum(net_revenue),2) FROM etl_fact_orders WHERE order_status='DELIVERED' GROUP BY 1 ORDER BY 1"),
      q("SELECT payment_mode, net_revenue FROM mart_payment_mode_revenue ORDER BY 1"))
check("delivery SLA per store", q("""SELECT store_id, round(avg(delivery_minutes),2), round(100.0*count(*) FILTER (WHERE within_15_min)/count(*),2)
      FROM etl_fact_orders WHERE order_status='DELIVERED' GROUP BY 1 ORDER BY 1"""),
      q("SELECT store_id, avg_delivery_minutes, pct_within_15_min FROM mart_delivery_sla ORDER BY 1"))
print("\nRESULT:", "ETL and ELT match" if not bad else f"{bad} difference(s)")
sys.exit(1 if bad else 0)
