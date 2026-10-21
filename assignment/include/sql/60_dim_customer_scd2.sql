-- BONUS: SCD Type 2 customer dimension. Keeps loyalty-tier history: one row per (customer, tier period).
-- Re-running with unchanged data changes nothing (idempotent). A tier change closes the old row and opens a new one.
CREATE TABLE IF NOT EXISTS dim_customer_scd2 (
  customer_id VARCHAR, loyalty_tier VARCHAR, city VARCHAR,
  valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN);

CREATE OR REPLACE TEMP VIEW scd_src AS
  SELECT customer_id, loyalty_tier, city FROM stg_customers WHERE dq_status <> 'QUARANTINE';

-- 1. close current rows whose tier changed
UPDATE dim_customer_scd2 d SET valid_to = now(), is_current = FALSE
WHERE d.is_current
  AND EXISTS (SELECT 1 FROM scd_src s WHERE s.customer_id = d.customer_id
              AND s.loyalty_tier IS DISTINCT FROM d.loyalty_tier);

-- 2. open a new current row for new customers and for customers just closed
INSERT INTO dim_customer_scd2
SELECT s.customer_id, s.loyalty_tier, s.city, now(), TIMESTAMP '9999-12-31', TRUE
FROM scd_src s
WHERE NOT EXISTS (SELECT 1 FROM dim_customer_scd2 d WHERE d.customer_id = s.customer_id AND d.is_current);
