-- STAGING products
CREATE OR REPLACE VIEW v_products_typed AS
WITH b AS (
  SELECT rowid AS _rid, * FROM raw_products
  WHERE _source_file = 'products.csv'
    AND _loaded_at = (SELECT max(_loaded_at) FROM raw_products WHERE _source_file = 'products.csv')
), t AS (
  SELECT *, row_number() OVER (PARTITION BY product_id ORDER BY _rid) AS rn,
         try_cast(nullif(trim(mrp), '') AS DOUBLE) AS mrp_n,
         try_cast(nullif(trim(cost_price), '') AS DOUBLE) AS cost_n,
         try_cast(nullif(trim(launch_date), '') AS TIMESTAMP) AS launch_ts
  FROM b
)
SELECT *,
  concat_ws('; ',
    CASE WHEN (trim(product_id) = '') IS TRUE THEN 'product_id_not_null' END,
    CASE WHEN (rn > 1) IS TRUE THEN 'product_id_unique' END,
    CASE WHEN (trim(category) = '') IS TRUE THEN 'category_not_null' END,
    CASE WHEN coalesce(NOT (mrp_n > 0), TRUE) THEN 'mrp_positive' END,
    CASE WHEN coalesce(NOT (cost_n > 0), TRUE) THEN 'cost_price_positive' END,
    CASE WHEN (cost_n > mrp_n) IS TRUE THEN 'cost_not_above_mrp' END,
    CASE WHEN (launch_ts > TIMESTAMP '${AS_OF}') IS TRUE THEN 'launch_date_not_future' END) AS failure_reason,
  '' AS warning_reason
FROM t;

DELETE FROM elt_quarantine WHERE run_id = '${RUN_ID}' AND source_file = 'products';
INSERT INTO elt_quarantine
  SELECT '${RUN_ID}', 'products', product_id, failure_reason, now() FROM v_products_typed WHERE failure_reason <> '';

CREATE OR REPLACE TABLE stg_products AS
SELECT trim(product_id) AS product_id, trim(product_name) AS product_name, trim(category) AS category,
       trim(sub_category) AS sub_category, trim(brand) AS brand, mrp_n AS mrp, cost_n AS cost_price,
       trim(unit) AS unit, lower(trim(is_perishable)) = 'true' AS is_perishable, CAST(launch_ts AS DATE) AS launch_date,
       CASE WHEN failure_reason <> '' THEN 'QUARANTINE' WHEN warning_reason <> '' THEN 'WARNING' ELSE 'PASS' END AS dq_status,
       failure_reason, warning_reason, _source_file, _run_id, _loaded_at
FROM v_products_typed
WHERE rn = 1;
