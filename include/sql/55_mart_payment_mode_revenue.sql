-- MART (helper for Q4): revenue by payment mode
CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
SELECT o.payment_mode, count(*) AS delivered_orders, round(sum(o.net_revenue), 2) AS net_revenue
FROM stg_orders o
WHERE o.dq_status <> 'QUARANTINE' AND o.order_status = 'DELIVERED'
GROUP BY 1 ORDER BY 1;
