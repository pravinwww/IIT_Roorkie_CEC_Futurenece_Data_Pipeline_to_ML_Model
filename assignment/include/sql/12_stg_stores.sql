-- STAGING stores (a blank/invalid zone is a Warning: the store loads with zone = 'UNASSIGNED')
CREATE OR REPLACE VIEW v_stores_typed AS
WITH b AS (
  SELECT rowid AS _rid, * FROM raw_stores
  WHERE _source_file = 'stores.csv'
    AND _loaded_at = (SELECT max(_loaded_at) FROM raw_stores WHERE _source_file = 'stores.csv')
), t AS (
  SELECT *, row_number() OVER (PARTITION BY store_id ORDER BY _rid) AS rn,
         try_cast(nullif(trim(capacity_orders_per_day), '') AS DOUBLE) AS cap_n,
         try_cast(nullif(trim(open_date), '') AS TIMESTAMP) AS open_ts
  FROM b
)
SELECT *,
  concat_ws('; ',
    CASE WHEN (trim(store_id) = '') IS TRUE THEN 'store_id_not_null' END,
    CASE WHEN (rn > 1) IS TRUE THEN 'store_id_unique' END) AS failure_reason,
  concat_ws('; ',
    CASE WHEN (trim(zone) NOT IN ('North', 'South', 'East', 'West')) IS TRUE THEN 'zone_valid' END,
    CASE WHEN coalesce(NOT (cap_n > 0), TRUE) THEN 'capacity_positive' END,
    CASE WHEN (NOT regexp_full_match(trim(pincode), '\d{6}')) IS TRUE THEN 'pincode_6_digits' END) AS warning_reason
FROM t;

DELETE FROM elt_quarantine WHERE run_id = '${RUN_ID}' AND source_file = 'stores';
INSERT INTO elt_quarantine
  SELECT '${RUN_ID}', 'stores', store_id, failure_reason, now() FROM v_stores_typed WHERE failure_reason <> '';

CREATE OR REPLACE TABLE stg_stores AS
SELECT trim(store_id) AS store_id, trim(store_name) AS store_name,
       CASE WHEN trim(zone) IN ('North', 'South', 'East', 'West') THEN trim(zone) ELSE 'UNASSIGNED' END AS zone,
       trim(area) AS area, trim(pincode) AS pincode, CAST(open_ts AS DATE) AS open_date,
       trim(manager_name) AS manager_name, cap_n AS capacity_orders_per_day,
       CASE WHEN failure_reason <> '' THEN 'QUARANTINE' WHEN warning_reason <> '' THEN 'WARNING' ELSE 'PASS' END AS dq_status,
       failure_reason, warning_reason, _source_file, _run_id, _loaded_at
FROM v_stores_typed
WHERE rn = 1;
