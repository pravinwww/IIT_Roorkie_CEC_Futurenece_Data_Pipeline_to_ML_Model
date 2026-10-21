-- The six business questions, answered with ONE simple query each on the ETL tables.
-- Q1 Daily net revenue by zone and by store (DELIVERED only, after discount)
SELECT order_date, zone, store_id, store_name, net_revenue FROM etl_daily_zone_revenue ORDER BY order_date, zone, store_id;
--   by zone:
SELECT order_date, zone, round(sum(net_revenue), 2) AS net_revenue FROM etl_daily_zone_revenue GROUP BY ALL ORDER BY 1, 2;
-- Q2 Revenue and gross margin by product category
SELECT * FROM etl_category_margin ORDER BY category;
-- Q3 Average delivery time per store and % of delivered orders within 15 minutes
SELECT store_id, round(avg(delivery_minutes), 2) AS avg_delivery_minutes,
       round(100.0 * avg(within_15_min::INT), 2) AS pct_within_15_min
FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 1;
-- Q4 Revenue by payment mode, and the total amount that does not reconcile with the gateway
SELECT payment_mode, round(sum(net_revenue), 2) AS net_revenue FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 1;
SELECT mismatch_type, count(*) AS mismatches, round(sum(unreconciled_amount), 2) AS unreconciled_amount FROM etl_payment_reconciliation GROUP BY ROLLUP(1) ORDER BY 1;
-- Q5 Top 10 customers by spend with loyalty tier (masked contact data only)
SELECT c.customer_id, c.customer_name_masked, c.loyalty_tier, c.email_masked, c.phone_masked, round(sum(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id) WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY total_spend DESC LIMIT 10;
-- Q6 Rows rejected today, by file and reason (latest run)
SELECT source_file, failure_reason, rows_rejected FROM etl_rejected_rows
WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_pravin' ORDER BY start_time DESC LIMIT 1)
ORDER BY source_file, rows_rejected DESC;
