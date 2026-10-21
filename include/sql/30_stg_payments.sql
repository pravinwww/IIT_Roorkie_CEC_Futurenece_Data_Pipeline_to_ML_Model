-- STAGING payments. Order amounts come from the orders files as received (increment rows override base rows).
CREATE OR REPLACE VIEW v_order_latest AS
SELECT order_id, a AS order_amount, mode_n AS payment_mode FROM (
  SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY (src = 'orders_increment') DESC, _source_file DESC) AS pri
  FROM v_orders_typed WHERE rn = 1
) WHERE pri = 1;

CREATE OR REPLACE VIEW v_payments_typed AS
WITH b AS (
  SELECT rowid AS _rid, * FROM raw_payments
  WHERE _source_file = 'payments.csv'
    AND _loaded_at = (SELECT max(_loaded_at) FROM raw_payments WHERE _source_file = 'payments.csv')
), t AS (
  SELECT b.*, row_number() OVER (PARTITION BY b.payment_id ORDER BY b._rid) AS rn,
    try_cast(nullif(trim(b.amount_paid), '') AS DOUBLE) AS amt,
    try_cast(nullif(trim(b.payment_ts), '') AS TIMESTAMP) AS ts,
    trim(b.payment_status) AS status_n,
    CASE lower(trim(b.payment_mode)) WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card'
         WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
         WHEN 'wallet' THEN 'Wallet' END AS mode_n,
    o.order_amount AS o_amount, o.payment_mode AS o_mode, (o.order_id IS NOT NULL) AS order_found
  FROM b LEFT JOIN v_order_latest o ON o.order_id = b.order_id
)
SELECT *,
  concat_ws('; ',
    CASE WHEN (trim(payment_id) = '') IS TRUE THEN 'payment_id_not_null' END,
    CASE WHEN (rn > 1) IS TRUE THEN 'payment_id_unique' END,
    CASE WHEN (ts IS NULL) IS TRUE THEN 'payment_ts_parses' END,
    CASE WHEN (status_n NOT IN ('SUCCESS', 'REFUNDED', 'FAILED')) IS TRUE THEN 'payment_status_valid' END,
    CASE WHEN (mode_n IS NULL) IS TRUE THEN 'payment_mode_valid' END,
    CASE WHEN (amt IS NULL) IS TRUE THEN 'amount_numeric' END,
    CASE WHEN (amt <= 0) IS TRUE THEN 'amount_positive' END,
    CASE WHEN (NOT order_found) IS TRUE THEN 'payment_order_exists' END,
    CASE WHEN (order_found AND status_n = 'SUCCESS' AND abs(amt - o_amount) > 0.01) IS TRUE THEN 'success_amount_matches_order' END,
    CASE WHEN (ts > TIMESTAMP '${AS_OF}') IS TRUE THEN 'payment_ts_not_future' END) AS failure_reason,
  concat_ws('; ',
    CASE WHEN (mode_n IS DISTINCT FROM 'Cash on Delivery' AND trim(gateway_ref) = '') IS TRUE THEN 'gateway_ref_present_for_online' END,
    CASE WHEN (mode_n IS NOT NULL AND payment_mode <> mode_n) IS TRUE THEN 'payment_mode_format' END,
    CASE WHEN (order_found AND mode_n IS NOT NULL AND o_mode IS NOT NULL AND mode_n <> o_mode) IS TRUE THEN 'payment_mode_matches_order' END) AS warning_reason
FROM t;

DELETE FROM elt_quarantine WHERE run_id = '${RUN_ID}' AND source_file = 'payments';
INSERT INTO elt_quarantine
  SELECT '${RUN_ID}', 'payments', payment_id, failure_reason, now() FROM v_payments_typed WHERE failure_reason <> '';

CREATE OR REPLACE TABLE stg_payments AS
SELECT trim(payment_id) AS payment_id, trim(order_id) AS order_id, ts AS payment_ts, mode_n AS payment_mode,
       amt AS amount_paid, status_n AS payment_status, nullif(trim(gateway_ref), '') AS gateway_ref,
       CASE WHEN failure_reason <> '' THEN 'QUARANTINE' WHEN warning_reason <> '' THEN 'WARNING' ELSE 'PASS' END AS dq_status,
       failure_reason, warning_reason, _source_file, _run_id, _loaded_at
FROM v_payments_typed
WHERE rn = 1;
