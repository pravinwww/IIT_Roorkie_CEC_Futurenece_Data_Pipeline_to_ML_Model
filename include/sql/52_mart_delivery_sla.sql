-- MART: Q3 average delivery time per store and % of delivered orders within 15 minutes
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT o.store_id, s.store_name, s.zone, count(*) AS delivered_orders,
       round(avg(o.delivery_minutes), 2) AS avg_delivery_minutes,
       round(100.0 * count(*) FILTER (WHERE o.within_15_min) / count(*), 2) AS pct_within_15_min
FROM stg_orders o JOIN stg_stores s ON s.store_id = o.store_id
WHERE o.dq_status <> 'QUARANTINE' AND o.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY 1;
