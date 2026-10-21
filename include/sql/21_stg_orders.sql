-- STAGING orders, step 3: build stg_orders from the base file (one row per order_id).
-- Duplicate rows are not kept in staging; they are recorded in elt_quarantine.
DELETE FROM elt_quarantine WHERE run_id = '${RUN_ID}' AND source_file = 'orders';
INSERT INTO elt_quarantine
  SELECT '${RUN_ID}', 'orders', order_id, failure_reason, now()
  FROM v_orders_stage WHERE src = 'orders' AND failure_reason <> '';

CREATE OR REPLACE TABLE stg_orders AS
SELECT order_id, order_ts, order_date, customer_id, store_id, product_id, quantity, unit_price, discount_pct,
       order_amount, payment_mode, order_status, channel, delivery_minutes, net_revenue, gross_margin,
       within_15_min, dq_status, failure_reason, warning_reason, source_file, _run_id, _loaded_at
FROM v_orders_stage
WHERE src = 'orders' AND rn = 1;
