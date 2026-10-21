-- BONUS: store capacity check. Daily orders per store vs capacity_orders_per_day (0 or missing capacity = cannot check).
CREATE OR REPLACE TABLE mart_store_capacity AS
SELECT o.order_date, o.store_id, s.store_name, s.capacity_orders_per_day AS capacity,
       count(*) AS orders_that_day,
       round(100.0 * count(*) / nullif(s.capacity_orders_per_day, 0), 2) AS utilisation_pct,
       CASE WHEN (s.capacity_orders_per_day IS NULL OR s.capacity_orders_per_day <= 0) THEN 'CAPACITY_UNKNOWN'
            WHEN count(*) > s.capacity_orders_per_day THEN 'OVER_CAPACITY'
            WHEN count(*) > 0.8 * s.capacity_orders_per_day THEN 'NEAR_CAPACITY' ELSE 'OK' END AS capacity_status
FROM stg_orders o JOIN stg_stores s ON s.store_id = o.store_id
WHERE o.dq_status <> 'QUARANTINE'
GROUP BY 1, 2, 3, 4 ORDER BY 1, 2;
