-- Volume check. Row counts per file are recorded on every run; a file is only FLAGGED when the Airflow Variable
-- nammamart_volume_check_enabled = true and it has < 10% of the largest row count of the last 5 runs.
INSERT INTO dq_results
SELECT '${RUN_ID}', '${DAG_ID}', f.src || '.volume_vs_recent_max', 'Completeness', 'Warning', cur.n,
       CASE WHEN ${VOLUME_CHECK} = 1 AND prev.m IS NOT NULL AND cur.n < 0.1 * prev.m THEN 1 ELSE 0 END,
       CASE WHEN ${VOLUME_CHECK} = 1 AND prev.m IS NOT NULL AND cur.n < 0.1 * prev.m THEN 'WARN' ELSE 'PASS' END,
       now()
FROM (VALUES ('orders', 'orders.csv', 'raw_orders'), ('payments', 'payments.csv', 'raw_payments'),
             ('customers', 'customers.csv', 'raw_customers'), ('products', 'products.csv', 'raw_products'),
             ('stores', 'stores.csv', 'raw_stores')) AS f(src, fname, tbl)
CROSS JOIN LATERAL (
  SELECT CASE f.src WHEN 'orders' THEN (SELECT count(*) FROM raw_orders WHERE _run_id = '${RUN_ID}' AND _source_file = f.fname)
                    WHEN 'payments' THEN (SELECT count(*) FROM raw_payments WHERE _run_id = '${RUN_ID}')
                    WHEN 'customers' THEN (SELECT count(*) FROM raw_customers WHERE _run_id = '${RUN_ID}')
                    WHEN 'products' THEN (SELECT count(*) FROM raw_products WHERE _run_id = '${RUN_ID}')
                    ELSE (SELECT count(*) FROM raw_stores WHERE _run_id = '${RUN_ID}') END AS n) cur
CROSS JOIN LATERAL (
  SELECT max(rows_checked) AS m FROM (
    SELECT rows_checked FROM dq_results WHERE dag_id = '${DAG_ID}' AND check_name = f.src || '.volume_vs_recent_max'
      AND run_id <> '${RUN_ID}' ORDER BY checked_at DESC LIMIT 5)) prev;
