-- MART: Q4 one row per order/payment mismatch, with a reason. unreconciled_amount sums to the total gap.
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH o AS (SELECT * FROM stg_orders WHERE dq_status <> 'QUARANTINE'),
     p AS (SELECT * FROM stg_payments),            -- payments as received, de-duplicated on payment_id
     a AS (
  SELECT o.order_id, p.payment_id,
         CASE WHEN o.order_status = 'CANCELLED' THEN CASE WHEN p.payment_id IS NOT NULL THEN 'PAYMENT_FOR_CANCELLED' END
              WHEN p.payment_id IS NULL THEN 'NO_PAYMENT'
              WHEN o.order_status = 'DELIVERED' AND p.payment_status = 'FAILED' THEN 'PAYMENT_FAILED'
              WHEN p.payment_status IN ('SUCCESS', 'REFUNDED') AND abs(p.amount_paid - o.order_amount) > 0.01 THEN 'AMOUNT_MISMATCH'
         END AS mismatch_type,
         o.order_amount, p.amount_paid, o.order_status, p.payment_status
  FROM o LEFT JOIN p ON p.order_id = o.order_id
)
SELECT '${RUN_ID}' AS run_id, order_id, payment_id, mismatch_type, order_amount, amount_paid,
       CASE mismatch_type
         WHEN 'PAYMENT_FOR_CANCELLED' THEN amount_paid
         WHEN 'NO_PAYMENT' THEN order_amount
         WHEN 'PAYMENT_FAILED' THEN order_amount
         ELSE round(abs(amount_paid - order_amount), 2) END AS unreconciled_amount,
       order_status, payment_status,
       CASE mismatch_type
         WHEN 'PAYMENT_FOR_CANCELLED' THEN 'Cancelled order has a payment'
         WHEN 'NO_PAYMENT' THEN 'No payment row in the gateway export'
         WHEN 'PAYMENT_FAILED' THEN 'Order delivered but the gateway payment FAILED'
         ELSE 'Amount paid differs from order_amount' END AS reason,
       now() AS reconciled_at
FROM a WHERE mismatch_type IS NOT NULL
UNION ALL
SELECT '${RUN_ID}', p.order_id, p.payment_id, 'PAYMENT_WITHOUT_CLEAN_ORDER', NULL, p.amount_paid, p.amount_paid,
       NULL, p.payment_status,
       'Money received but no clean order: ' || CASE WHEN EXISTS (SELECT 1 FROM v_orders_typed v WHERE v.order_id = p.order_id)
            THEN 'order rejected by quality checks (in quarantine)' ELSE 'order_id not present in orders files' END,
       now()
FROM p
WHERE p.payment_status IN ('SUCCESS', 'REFUNDED') AND NOT EXISTS (SELECT 1 FROM o WHERE o.order_id = p.order_id);
