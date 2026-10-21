-- MART: Q1 daily net revenue by zone and store (DELIVERED only, after discount) + delivery KPIs per day/store
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT o.order_date, s.zone, o.store_id, s.store_name,
       count(*) AS total_orders,
       count(*) FILTER (WHERE o.order_status = 'DELIVERED') AS delivered_orders,
       count(*) FILTER (WHERE o.order_status = 'RETURNED') AS returned_orders,
       count(*) FILTER (WHERE o.order_status = 'CANCELLED') AS cancelled_orders,
       round(sum(o.net_revenue), 2) AS net_revenue,
       round(avg(o.delivery_minutes) FILTER (WHERE o.order_status = 'DELIVERED'), 2) AS avg_delivery_minutes,
       round(100.0 * count(*) FILTER (WHERE o.within_15_min) / nullif(count(*) FILTER (WHERE o.order_status = 'DELIVERED'), 0), 2) AS pct_within_15_min
FROM stg_orders o JOIN stg_stores s ON s.store_id = o.store_id
WHERE o.dq_status <> 'QUARANTINE'
GROUP BY ALL ORDER BY 1, 2, 3;
