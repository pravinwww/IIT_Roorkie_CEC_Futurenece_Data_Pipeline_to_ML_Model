-- Quality scorecard: one dq_results row per check, per run. Re-running the same run_id replaces its rows.
DELETE FROM dq_results WHERE run_id = '${RUN_ID}' AND dag_id = '${DAG_ID}';

CREATE OR REPLACE TEMP VIEW v_dq_rows AS
          SELECT 'customers' AS src, failure_reason, warning_reason FROM v_customers_typed
UNION ALL SELECT 'products', failure_reason, warning_reason FROM v_products_typed
UNION ALL SELECT 'stores', failure_reason, warning_reason FROM v_stores_typed
UNION ALL SELECT src, failure_reason, warning_reason FROM v_orders_typed
UNION ALL SELECT 'payments', failure_reason, warning_reason FROM v_payments_typed;

INSERT INTO dq_results
SELECT '${RUN_ID}', '${DAG_ID}', c.src || '.' || c.check_name, c.dimension, c.severity, n.rows_checked,
       (SELECT count(*) FROM v_dq_rows r
        WHERE r.src = c.src AND list_contains(string_split(r.failure_reason || '; ' || r.warning_reason, '; '), c.check_name)),
       'PENDING', now()
FROM dq_check_catalog c JOIN (SELECT src, count(*) AS rows_checked FROM v_dq_rows GROUP BY 1) n ON n.src = c.src;

-- file-level timeliness check: the newest (non-future) order must not be older than 1 day at the extract time
INSERT INTO dq_results
SELECT '${RUN_ID}', '${DAG_ID}', src || '.latest_order_not_stale', 'Timeliness', 'Warning', 1,
       CAST(max(ts) FILTER (WHERE ts <= TIMESTAMP '${AS_OF}') IS NULL
            OR max(ts) FILTER (WHERE ts <= TIMESTAMP '${AS_OF}') < TIMESTAMP '${AS_OF}' - INTERVAL 1 DAY AS INTEGER), 'PENDING', now()
FROM v_orders_typed GROUP BY src;

UPDATE dq_results SET status = CASE WHEN rows_failed = 0 THEN 'PASS' WHEN severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END
WHERE run_id = '${RUN_ID}' AND dag_id = '${DAG_ID}';
