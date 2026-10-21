-- MART: Q5 top 10 customers by delivered spend, with loyalty tier. Only masked contact data is shown.
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT row_number() OVER (ORDER BY sum(o.net_revenue) DESC, c.customer_id) AS rank,
       c.customer_id,
       CASE WHEN c.dq_status = 'QUARANTINE' THEN 'UNKNOWN' ELSE c.customer_name_masked END AS customer_name_masked,
       CASE WHEN c.dq_status = 'QUARANTINE' THEN '***' ELSE c.email_masked END AS email_masked,
       CASE WHEN c.dq_status = 'QUARANTINE' THEN '***' ELSE c.phone_masked END AS phone_masked,
       CASE WHEN c.dq_status = 'QUARANTINE' THEN 'UNKNOWN' ELSE c.loyalty_tier END AS loyalty_tier,
       count(*) AS delivered_orders, round(sum(o.net_revenue), 2) AS total_spend
FROM stg_orders o JOIN stg_customers c ON c.customer_id = o.customer_id
WHERE o.dq_status <> 'QUARANTINE' AND o.order_status = 'DELIVERED'
GROUP BY c.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier, c.dq_status
ORDER BY total_spend DESC, c.customer_id LIMIT 10;
