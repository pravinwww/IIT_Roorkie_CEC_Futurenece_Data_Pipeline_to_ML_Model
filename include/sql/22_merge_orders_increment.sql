-- INCREMENTAL LOAD: upsert the next-day file(s) into stg_orders on order_id.
--   matched      -> the old row is overwritten (e.g. DELIVERED -> RETURNED)
--   not matched  -> the new order is inserted
-- Only increment rows that pass the critical checks are applied; the rest go to elt_quarantine.
DELETE FROM elt_quarantine WHERE run_id = '${RUN_ID}' AND source_file = 'orders_increment';
INSERT INTO elt_quarantine
  SELECT '${RUN_ID}', 'orders_increment', order_id, failure_reason, now()
  FROM v_orders_stage WHERE src = 'orders_increment' AND failure_reason <> '';

MERGE INTO stg_orders AS t
USING (
  SELECT * EXCLUDE (src, rn, pri) FROM (
    SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY source_file DESC) AS pri
    FROM v_orders_stage WHERE src = 'orders_increment' AND rn = 1 AND failure_reason = ''
  ) WHERE pri = 1
) AS s ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
  order_ts = s.order_ts, order_date = s.order_date, customer_id = s.customer_id, store_id = s.store_id,
  product_id = s.product_id, quantity = s.quantity, unit_price = s.unit_price, discount_pct = s.discount_pct,
  order_amount = s.order_amount, payment_mode = s.payment_mode, order_status = s.order_status,
  channel = s.channel, delivery_minutes = s.delivery_minutes, net_revenue = s.net_revenue,
  gross_margin = s.gross_margin, within_15_min = s.within_15_min, dq_status = s.dq_status,
  failure_reason = s.failure_reason, warning_reason = s.warning_reason, source_file = s.source_file,
  _run_id = s._run_id, _loaded_at = s._loaded_at
WHEN NOT MATCHED THEN INSERT
  (order_id, order_ts, order_date, customer_id, store_id, product_id, quantity, unit_price, discount_pct,
   order_amount, payment_mode, order_status, channel, delivery_minutes, net_revenue, gross_margin,
   within_15_min, dq_status, failure_reason, warning_reason, source_file, _run_id, _loaded_at)
  VALUES (s.order_id, s.order_ts, s.order_date, s.customer_id, s.store_id, s.product_id, s.quantity,
   s.unit_price, s.discount_pct, s.order_amount, s.payment_mode, s.order_status, s.channel,
   s.delivery_minutes, s.net_revenue, s.gross_margin, s.within_15_min, s.dq_status, s.failure_reason,
   s.warning_reason, s.source_file, s._run_id, s._loaded_at);
