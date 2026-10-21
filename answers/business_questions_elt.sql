-- The six business questions, answered with ONE simple query each on the ELT mart tables.
-- Q1 Daily net revenue by zone and by store (DELIVERED only, after discount)
SELECT order_date, zone, store_id, store_name, net_revenue FROM mart_daily_store_kpis WHERE delivered_orders > 0 ORDER BY order_date, zone, store_id;
--   by zone:
SELECT order_date, zone, round(sum(net_revenue), 2) AS net_revenue FROM mart_daily_store_kpis GROUP BY ALL ORDER BY 1, 2;
-- Q2 Revenue and gross margin by product category
SELECT * FROM mart_category_margin ORDER BY category;
-- Q3 Average delivery time per store and % of delivered orders within 15 minutes
SELECT store_id, avg_delivery_minutes, pct_within_15_min FROM mart_delivery_sla ORDER BY 1;
-- Q4 Revenue by payment mode, and the total amount that does not reconcile with the gateway
SELECT * FROM mart_payment_mode_revenue ORDER BY 1;
SELECT mismatch_type, count(*) AS mismatches, round(sum(unreconciled_amount), 2) AS unreconciled_amount FROM mart_payment_reconciliation GROUP BY ROLLUP(1) ORDER BY 1;
-- Q5 Top 10 customers by spend with loyalty tier (masked contact data only)
SELECT rank, customer_id, customer_name_masked, loyalty_tier, email_masked, phone_masked, total_spend FROM mart_top_customers ORDER BY rank;
-- Q6 Rows rejected today, by file and reason (latest run)
SELECT source_file, failure_reason, rows_rejected FROM mart_rejected_rows
WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_elt_pravin' ORDER BY start_time DESC LIMIT 1)
ORDER BY source_file, rows_rejected DESC;
