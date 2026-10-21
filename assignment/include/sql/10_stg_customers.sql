-- STAGING customers: cast, standardise, dedupe, MASK PII, apply quality rules. SQL only.
CREATE OR REPLACE VIEW v_customers_typed AS
WITH b AS (
  SELECT rowid AS _rid, * FROM raw_customers
  WHERE _source_file = 'customers.csv'
    AND _loaded_at = (SELECT max(_loaded_at) FROM raw_customers WHERE _source_file = 'customers.csv')
), t AS (
  SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY _rid) AS rn,
         try_cast(nullif(trim(signup_date), '') AS TIMESTAMP) AS signup_ts,
         upper(left(trim(loyalty_tier), 1)) || lower(substr(trim(loyalty_tier), 2)) AS tier_clean
  FROM b
)
SELECT *,
  concat_ws('; ',
    CASE WHEN (trim(customer_id) = '') IS TRUE THEN 'customer_id_not_null' END,
    CASE WHEN (rn > 1) IS TRUE THEN 'customer_id_unique' END,
    CASE WHEN (signup_ts IS NULL) IS TRUE THEN 'signup_date_parses' END,
    CASE WHEN (signup_ts > TIMESTAMP '${AS_OF}') IS TRUE THEN 'signup_date_not_future' END) AS failure_reason,
  concat_ws('; ',
    CASE WHEN (NOT regexp_matches(trim(email), '^[^@\s]+@[^@\s]+\.[^@\s]+$')) IS TRUE THEN 'email_valid' END,
    CASE WHEN (NOT regexp_full_match(trim(phone), '\d{10}')) IS TRUE THEN 'phone_10_digits' END,
    CASE WHEN (NOT regexp_full_match(trim(pincode), '\d{6}')) IS TRUE THEN 'pincode_6_digits' END,
    CASE WHEN (trim(city) = '') IS TRUE THEN 'city_not_null' END,
    CASE WHEN (loyalty_tier <> tier_clean) IS TRUE THEN 'loyalty_tier_casing' END,
    CASE WHEN (trim(is_active) NOT IN ('Y', 'N')) IS TRUE THEN 'is_active_valid' END) AS warning_reason
FROM t;

DELETE FROM elt_quarantine WHERE run_id = '${RUN_ID}' AND source_file = 'customers';
INSERT INTO elt_quarantine
  SELECT '${RUN_ID}', 'customers', customer_id, failure_reason, now() FROM v_customers_typed WHERE failure_reason <> '';

CREATE OR REPLACE TABLE stg_customers AS
SELECT trim(customer_id) AS customer_id,
       CASE WHEN len(string_split(trim(regexp_replace(full_name, '\s+', ' ', 'g')), ' ')) > 1
            THEN string_split(trim(regexp_replace(full_name, '\s+', ' ', 'g')), ' ')[1] || ' ' ||
                 left(string_split(trim(regexp_replace(full_name, '\s+', ' ', 'g')), ' ')[-1], 1) || '.'
            ELSE trim(full_name) END AS customer_name_masked,
       CASE WHEN instr(email, '@') = 0 THEN '***'
            ELSE substr(split_part(trim(email), '@', 1), 1, 2) || '***@' || substr(trim(email), instr(trim(email), '@') + 1)
       END AS email_masked,
       CASE WHEN len(regexp_replace(phone, '\D', '', 'g')) < 5 THEN repeat('*', len(regexp_replace(phone, '\D', '', 'g')))
            ELSE left(regexp_replace(phone, '\D', '', 'g'), 2) ||
                 repeat('*', len(regexp_replace(phone, '\D', '', 'g')) - 4) ||
                 right(regexp_replace(phone, '\D', '', 'g'), 2) END AS phone_masked,
       trim(city) AS city, trim(area) AS area, trim(pincode) AS pincode,
       CAST(signup_ts AS DATE) AS signup_date, tier_clean AS loyalty_tier, trim(is_active) AS is_active,
       CASE WHEN failure_reason <> '' THEN 'QUARANTINE' WHEN warning_reason <> '' THEN 'WARNING' ELSE 'PASS' END AS dq_status,
       failure_reason, warning_reason, _source_file, _run_id, _loaded_at
FROM v_customers_typed
WHERE rn = 1;
