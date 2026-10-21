-- STAGING orders, step 1: typed + validated view over the LATEST loaded batch of every orders file
-- (orders.csv and any orders_*_increment.csv). Quality rules produce failure_reason / warning_reason.
CREATE OR REPLACE VIEW v_orders_typed AS
WITH latest AS (SELECT _source_file, max(_loaded_at) AS m FROM raw_orders GROUP BY 1),
b AS (
  SELECT rowid AS _rid, r.* FROM raw_orders r JOIN latest l ON r._source_file = l._source_file AND r._loaded_at = l.m
), t AS (
  SELECT *,
    CASE WHEN _source_file = 'orders.csv' THEN 'orders' ELSE 'orders_increment' END AS src,
    row_number() OVER (PARTITION BY _source_file, order_id ORDER BY _rid) AS rn,
    try_cast(nullif(trim(quantity), '') AS DOUBLE) AS q,
    try_cast(nullif(trim(unit_price), '') AS DOUBLE) AS u,
    try_cast(nullif(trim(discount_pct), '') AS DOUBLE) AS d,
    try_cast(nullif(trim(order_amount), '') AS DOUBLE) AS a,
    try_cast(nullif(trim(delivery_minutes), '') AS DOUBLE) AS dm,
    try_cast(nullif(trim(order_ts), '') AS TIMESTAMP) AS ts,
    upper(trim(order_status)) AS status_n,
    CASE lower(trim(payment_mode)) WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card'
         WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
         WHEN 'wallet' THEN 'Wallet' END AS mode_n
  FROM b
)
SELECT t.*,
  concat_ws('; ',
    CASE WHEN (trim(customer_id) = '') IS TRUE THEN 'customer_id_not_null' END,
    CASE WHEN (trim(order_ts) = '') IS TRUE THEN 'order_ts_not_null' END,
    CASE WHEN (status_n = 'DELIVERED' AND dm IS NULL) IS TRUE THEN 'delivered_has_delivery_minutes' END,
    CASE WHEN (rn > 1) IS TRUE THEN 'order_id_unique' END,
    CASE WHEN (ts IS NULL AND trim(order_ts) <> '') IS TRUE THEN 'order_ts_parses' END,
    CASE WHEN (status_n NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED')) IS TRUE THEN 'order_status_valid' END,
    CASE WHEN (mode_n IS NULL) IS TRUE THEN 'payment_mode_valid' END,
    CASE WHEN (q IS NULL OR u IS NULL OR d IS NULL OR a IS NULL) IS TRUE THEN 'numeric_fields_valid' END,
    CASE WHEN (d IS NOT NULL AND NOT (d >= 0 AND d <= 100)) IS TRUE THEN 'discount_pct_range' END,
    CASE WHEN (q <= 0) IS TRUE THEN 'quantity_positive' END,
    CASE WHEN (q IS NOT NULL AND u IS NOT NULL AND d IS NOT NULL AND a IS NOT NULL
               AND abs(round(q * u * (1 - d / 100), 2) - a) > 0.01) IS TRUE THEN 'amount_matches_formula' END,
    CASE WHEN (dm >= 120 OR dm <= 0) IS TRUE THEN 'delivery_minutes_plausible' END,
    CASE WHEN (trim(customer_id) <> '' AND NOT EXISTS
               (SELECT 1 FROM stg_customers c WHERE c.customer_id = t.customer_id)) IS TRUE
         THEN 'customer_exists' END,
    CASE WHEN (NOT EXISTS
               (SELECT 1 FROM stg_products p WHERE p.product_id = t.product_id)) IS TRUE
         THEN 'product_exists' END,
    CASE WHEN (NOT EXISTS
               (SELECT 1 FROM stg_stores s WHERE s.store_id = t.store_id)) IS TRUE
         THEN 'store_exists' END,
    CASE WHEN (ts > TIMESTAMP '${AS_OF}') IS TRUE THEN 'order_ts_not_future' END) AS failure_reason,
  concat_ws('; ',
    CASE WHEN (status_n IN ('DELIVERED', 'CANCELLED', 'RETURNED') AND order_status <> status_n) IS TRUE THEN 'order_status_casing' END,
    CASE WHEN (mode_n IS NOT NULL AND payment_mode <> mode_n) IS TRUE THEN 'payment_mode_format' END,
    CASE WHEN (SELECT abs(u - p.mrp) > 0.005 FROM stg_products p
               WHERE p.product_id = t.product_id AND p.dq_status <> 'QUARANTINE') IS TRUE THEN 'unit_price_matches_mrp' END
  ) AS warning_reason
FROM t;

-- Step 2: projection to the staging columns (net_revenue, gross_margin, within_15_min derived in SQL)
CREATE OR REPLACE VIEW v_orders_stage AS
SELECT trim(v.order_id) AS order_id, v.ts AS order_ts, CAST(v.ts AS DATE) AS order_date,
       trim(v.customer_id) AS customer_id, trim(v.store_id) AS store_id, trim(v.product_id) AS product_id,
       CAST(v.q AS BIGINT) AS quantity, v.u AS unit_price, v.d AS discount_pct, v.a AS order_amount,
       v.mode_n AS payment_mode, v.status_n AS order_status, trim(v.channel) AS channel, v.dm AS delivery_minutes,
       CASE WHEN v.status_n = 'DELIVERED' THEN round(v.a, 2) ELSE 0 END AS net_revenue,
       CASE WHEN v.status_n = 'DELIVERED' THEN round(v.a - v.q * CASE WHEN p.dq_status = 'QUARANTINE' THEN NULL ELSE p.cost_price END, 2) ELSE 0 END AS gross_margin,
       CASE WHEN v.status_n = 'DELIVERED' THEN v.dm <= 15 END AS within_15_min,
       CASE WHEN v.failure_reason <> '' THEN 'QUARANTINE' WHEN v.warning_reason <> '' THEN 'WARNING' ELSE 'PASS' END AS dq_status,
       v.failure_reason, v.warning_reason, v._source_file AS source_file, v._run_id, v._loaded_at,
       v.src, v.rn
FROM v_orders_typed v LEFT JOIN stg_products p ON p.product_id = v.product_id;
