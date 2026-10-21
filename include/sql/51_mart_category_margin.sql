-- MART: Q2 revenue and gross margin by product category (margin = revenue - quantity * cost_price)
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT CASE WHEN p.dq_status = 'QUARANTINE' THEN 'UNKNOWN' ELSE p.category END AS category,
       round(sum(o.net_revenue), 2) AS net_revenue,
       round(sum(o.net_revenue - o.quantity * CASE WHEN p.dq_status = 'QUARANTINE' THEN NULL ELSE p.cost_price END), 2) AS gross_margin,
       round(100 * sum(o.net_revenue - o.quantity * CASE WHEN p.dq_status = 'QUARANTINE' THEN NULL ELSE p.cost_price END) / sum(o.net_revenue), 2) AS margin_pct
FROM stg_orders o JOIN stg_products p ON p.product_id = o.product_id
WHERE o.dq_status <> 'QUARANTINE' AND o.order_status = 'DELIVERED'
GROUP BY 1 ORDER BY 1;
