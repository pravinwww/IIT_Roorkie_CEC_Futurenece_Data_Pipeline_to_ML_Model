# NammaMart ETL & ELT Pipeline: Submission

Author: Pravin Singh. Files: `dags/nammamart_etl_pravin.py`, `dags/nammamart_elt_pravin.py`, helpers in `include/nammamart/`, SQL in `include/sql/`.
All numbers below come from running the delivered code on the provided dataset (September files plus the 1 October increment).

> **How the evidence was produced.** Both DAGs were run in a real Airflow 3.2.2 instance (api-server, scheduler, dag-processor) on the delivered code, using the scheduler's own 09:00 IST run, and experiments 4, 6 and 10 were run and observed in the Airflow UI. Experiments 1-3, 5, 7-9 were reproduced by calling the same task functions outside Airflow (`scripts/run_experiments_local.py`). Screenshots `01-03`, `07-11` are captured from the Airflow UI. `04` (quarantine CSV) and `05`, `06` (output of `scripts/show_results.py`) are images rendered from the real run outputs; the text is in `screenshots/show_results_output.txt`. Airflow was installed with pip in a Linux sandbox because Docker/Astro were not available there; the DAG files and code are identical to what runs under `astro dev start`. **Bug found and fixed by the real run:** after a task failed, the DAG run still showed green because `run_audit` (trigger_rule `all_done`) is the only final task and always succeeded. `run_audit` now writes its audit row and then raises `AirflowFailException` if any task did not succeed, so the run turns red (see experiment 4).

## Screenshots

All in `screenshots/`: `01_etl_graph`, `02_elt_graph` (Graph view, green), `03_schedule` (09:00 IST schedule), `04_quarantine_file`, `05_dq_results_pipeline_runs`, `06_payment_reconciliation`, plus experiment evidence `07`-`11`.

## Contents
0. Part 0: Data profiling and Data Issues Log
1. Part A and B: what was built (ETL, ELT) and how the two compare
2. Part C: Scheduling answers
3. Part D: Data quality dimensions
4. Part E: Data governance
5. Part F: Observability tests
6. Part G: Recommendation to the CTO
7. Business questions: see `answers/ANSWERS.md` (SQL + result, from both pipelines)

---

## Part 0: Discovery and data profiling

### Profile summary
| File | Rows | Key | Blanks | Duplicate keys |
| --- | --- | --- | --- | --- |
| orders.csv | 2,412 | order_id | customer_id 18, delivery_minutes 164 (72 are non-delivered orders), order_ts 1 (+5 unparseable text) | 12 (exact duplicate rows) |
| payments.csv | 2,228 | payment_id | gateway_ref 301 (all Cash on Delivery: expected) | 4 (exact duplicates) |
| customers.csv | 405 | customer_id | city 6 | 5 ids repeated, rows differ in phone/area/pincode |
| products.csv | 152 | product_id | category 3 | 2 ids repeated |
| stores.csv | 15 | store_id | zone 1 (S07) | 0 |
| increment | 160 | order_id | none | 0 (10 ids exist in orders = corrections, 150 new) |

Distinct values: `order_status` has DELIVERED 2182, CANCELLED 156, RETURNED 69, plus `Pending??` (3) and `delivered` (2). `payment_mode` has the 4 valid values plus `upi`, `UPI `, `CARD`, `cod`, `Cashh`. `loyalty_tier` has `BRONZE`, `bronze`, `SILVER`. Delivery minutes range from 8 to 1440. Order amounts range from -1593 to 9576.

Referential integrity: every `orders.customer_id` that is present exists in customers; 9 orders point to products and 5 orders to stores that are not in the masters; 8 payments point to orders that do not exist; 27 DELIVERED orders have no payment row; 42 delivered orders have a FAILED payment. Payments for CANCELLED orders: none. RETURNED orders have REFUNDED payments (68 of 69).

### Data Issues Log
| # | File | Row key(s) | Column | Problem | Dimension | Proposed rule | Severity |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | orders | 12 order_ids, e.g. ORD001462, ORD001326 | order_id | Exact duplicate rows | Uniqueness | order_id unique; keep first | Critical |
| 2 | orders | 18 rows, e.g. ORD002255, ORD000701 | customer_id | Blank | Completeness | not null | Critical |
| 3 | orders | 8 rows, e.g. ORD001554, ORD001800 | delivery_minutes | DELIVERED with no delivery time | Completeness | DELIVERED requires delivery_minutes | Critical |
| 4 | orders | 6 rows, e.g. ORD000403, ORD001610 | order_ts | Blank or unparseable (`N/A`, `2026-13-05 11:00:00`, `31/09/2026 10:00`) | Validity | timestamp must parse | Critical |
| 5 | orders | 7 rows, e.g. ORD002119 | order_ts | Future dates (2026-10-21, 2026-11-02, 2027-01-01) vs a September file | Timeliness | order_ts <= run time | Critical |
| 6 | orders | 5 rows (3 `Pending??`, 2 `delivered`) | order_status | Not in DELIVERED/CANCELLED/RETURNED. `delivered` is a casing error (fixable); `Pending??` is invalid | Validity | upper-case then check list; reject unknown | Critical |
| 7 | orders | 9 rows, e.g. ORD001301 payment_mode: `upi`, `UPI `, `CARD`, `cod`, `Cashh` | payment_mode | Casing, spaces, abbreviations, typo | Validity | trim, map to UPI/Card/Cash on Delivery/Wallet; reject unknown (`Cashh`) after mapping | Warning (fixable) / Critical (unmappable) |
| 8 | orders | 10 rows, e.g. ORD000469, ORD001296 | quantity | Zero or negative | Accuracy | quantity > 0 | Critical |
| 9 | orders | ORD002103, ORD001468, ORD001237 | discount_pct | 120, 150, 250 | Accuracy / Validity | 0 to 100 | Critical |
| 10 | orders | 6 rows, e.g. ORD001296 | order_amount | Negative amounts | Accuracy | amount >= 0 | Critical |
| 11 | orders | 15 rows, e.g. ORD002305, ORD002004 | order_amount | Does not equal qty x unit_price x (1 - disc/100) | Accuracy | recompute and compare, tolerance 0.01 | Critical |
| 12 | orders | 6 rows, e.g. ORD001037 | delivery_minutes | Above 120 (up to 1440 = a full day) | Accuracy | delivery_minutes < 120 | Critical |
| 13 | orders | 9 rows, e.g. ORD000070, ORD000986 | product_id | Not in products master | Consistency | id must exist in the master file | Critical |
| 14 | orders | 5 rows, e.g. ORD001799 | store_id | Not in stores master | Consistency | id must exist in the master file | Critical |
| 15 | orders | 20 rows | unit_price | Differs from the product master mrp | Consistency | Warning only; ask client if price can differ from mrp | Warning |
| 16 | payments | 4 payment_ids, e.g. PAY0000509 | payment_id | Exact duplicate rows | Uniqueness | payment_id unique | Critical |
| 17 | payments | 8 rows, e.g. PAY0002218 to PAY0002224 | order_id | Order not in orders (ids like ORD941071) | Consistency | order must exist | Critical |
| 18 | payments | 14 SUCCESS rows, e.g. PAY0001568, PAY0000354 | amount_paid | Not equal to order_amount | Consistency / Accuracy | SUCCESS amount = order_amount | Critical (goes to reconciliation) |
| 19 | payments | 10 rows, e.g. PAY0001568 | amount_paid | Zero or negative | Accuracy | amount_paid > 0 for SUCCESS | Critical |
| 20 | payments | 7 rows, e.g. PAY0000396 | payment_ts | Future dated | Timeliness | payment_ts <= run time | Critical |
| 21 | payments | 9 rows (`upi`, `UPI `, `cod`, `CARD`, `Cashh`) | payment_mode | Same casing and typo problems as orders | Validity | same mapping as orders | Warning |
| 22 | payments + orders | 27 DELIVERED orders | order_id | Delivered order with no payment row | Completeness | list in reconciliation table | Warning (reconciliation) |
| 23 | customers | 5 ids: C00036, C00103, C00226, C00252, C00313 | customer_id | Repeated id with different phone/area/pincode | Uniqueness | one row per id; decide which row wins (ask client) | Critical |
| 24 | customers | 12 rows, e.g. C00337 (`arjun.rao307example.com`, `N/A`) | email | No @ or incomplete domain | Validity | email regex | Warning |
| 25 | customers | 7 rows, e.g. C00151 (`996292312`) | phone | Fewer than 10 digits | Validity | 10 digits | Warning |
| 26 | customers | 3 rows, e.g. C00132 | pincode | Not 6 digits | Validity | 6 digits | Warning |
| 27 | customers | 8 rows (`BRONZE`, `bronze`, `SILVER`) | loyalty_tier | Inconsistent casing | Consistency | title-case | Warning |
| 28 | customers | 3 rows: C00400, C00398, C00038 | signup_date | Dated after run date (up to 2027-01-05) | Timeliness | signup_date <= run date | Critical |
| 29 | customers | 6 rows | city | Blank | Completeness | not null | Warning |
| 30 | products | 3 rows: P0051, P0060, P0068 | category | Blank | Completeness | not null | Critical |
| 31 | products | 2 ids: P0030, P0140 | product_id | Duplicate rows | Uniqueness | unique | Critical |
| 32 | products | P0109 | mrp | Zero or negative (-203) | Accuracy | mrp > 0 | Critical |
| 33 | products | 5 rows: P0019, P0073, P0074, P0109, P0117 | cost_price | cost_price > mrp (negative margin) | Accuracy | cost_price <= mrp | Critical |
| 34 | stores | S07 | zone | Blank (BTM Layout, probably South) | Completeness | zone in North/South/East/West; load with zone = UNASSIGNED, never guess | Warning |
| 35 | stores | S12 | capacity_orders_per_day | 0 | Accuracy | capacity > 0 | Warning |
| 36 | increment | 10 ids, e.g. ORD000017, ORD000271 | order_status | Corrections: DELIVERED to RETURNED (not an error; test upsert) | Timeliness / change data | MERGE on order_id | Info |

Every file has at least one issue (orders 15, payments 6, customers 7, products 4, stores 2). The increment file is clean; its 10 status corrections (DELIVERED to RETURNED) are the changes to apply.
No obvious "test entries" were found in ids, names or customers. The duplicated and invalid-status rows are the likely candidates.

### Questions I would ask the client before building
1. Is a negative or zero quantity a return or a data-entry error, and should it ever reduce revenue?
2. When the same customer_id appears twice with different phone/area/pincode, which row is correct: the latest, or the CRM master?
3. Should a RETURNED order (DELIVERED on 3 Sep, RETURNED on 1 Oct) reduce September revenue after the fact, or only be counted in October?
4. (extra) Store S07 has no zone and S12 has capacity 0: can ops confirm? Is 120 minutes the right cut-off for impossible delivery times? What happens to `Pending??` orders?

---

## Part A and B: what was built

### ETL DAG (`nammamart_etl_pravin`)
`extract_orders | extract_payments | extract_customers | extract_products | extract_stores` (5 parallel) -> `validate` -> `quarantine` and `transform` -> `load` -> `reconcile` -> `report` -> `run_audit` (trigger_rule=all_done).
* **Extract:** fails loudly if a file is missing or its columns changed (schema contract). Any `orders_*_increment.csv` in the data folder is read together with `orders.csv`.
* **Validate / quarantine:** Part D rules in pandas. Failed rows go to `include/quarantine/pravin/<file>_failed_<run_id>.csv` with `failure_reason`; reason counts go to `etl_rejected_rows`.
* **Transform:** trim, case-fix, map `upi`/`UPI ` to `UPI`, deduplicate, mask name/email/phone, join, derive `order_date`, `net_revenue`, `gross_margin`, `within_15_min`.
* **Load:** star schema `etl_fact_orders` + `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`. Idempotent: upsert by key (delete the keys being loaded, insert) inside one transaction. An order that is now rejected is removed from the fact. Increment rows overwrite the old order.
* **Reconcile:** `etl_payment_reconciliation`, one row per mismatch with a reason. **Report:** `etl_daily_zone_revenue`, `etl_category_margin`, printed in the task log.

### ELT DAG (`nammamart_elt_pravin`)
`extract_*` (5 parallel) -> `load_raw` -> `stg_customers` -> `stg_products` -> `stg_stores` -> `stg_orders` -> `load_raw_increment` -> `merge_stg_orders` -> `stg_payments` -> `dq_results` -> 7 mart tasks -> `run_audit`.
* **Raw** (`raw_*`): all columns text, blanks kept as blanks, plus `_loaded_at`, `_source_file`, `_run_id`. Append only.
* **Staging** (`stg_*`): SQL only (`include/sql/10..30_*.sql`): cast, standardise, dedupe, mask PII, quality rules, `dq_status` (PASS / WARNING / QUARANTINE) and `failure_reason`.
* **Increment:** `load_raw_increment` appends the file to `raw_orders`; `22_merge_orders_increment.sql` runs `MERGE INTO stg_orders ... ON order_id` (matched: update; not matched: insert).
* **Marts:** `mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_payment_reconciliation`, `mart_top_customers`, plus two helpers `mart_payment_mode_revenue` and `mart_rejected_rows`.
* DuckDB allows one writer, and both DAGs write the same warehouse file at 09:00, so every task opens the file with a retry loop instead of failing on a lock.

### Results on the provided data (September files + 1 October increment)
| Measure | ETL | ELT |
| --- | --- | --- |
| Clean orders | 2,458 (`etl_fact_orders`) | 2,458 (`stg_orders` not QUARANTINE) |
| Net revenue (DELIVERED) | 1,210,421.30 | 1,210,421.30 |
| September-only revenue | 1,130,769.10 | 1,130,769.10 |
| Rows rejected (distinct) | customers 8, products 10, orders 104, payments 43 | same |
| Increment (first load after September) | 150 inserted, 10 updated, 2,298 unchanged | 150 inserted, 10 updated |
| Second run, same inputs | 0 inserted, 0 updated, fact count unchanged | stg unchanged; `raw_*` doubled (correct) |

`scripts/compare_etl_elt.py` compares the two pipelines row by row (clean orders, revenue, rejected rows by reason, masked customers, `dq_results`, category margin, daily revenue, reconciliation, top 10, SLA). Result: **ETL and ELT match on every comparison.** Where they could differ, the reason is design, not error: ETL upserts (an old row survives if a later file leaves it out), while ELT rebuilds staging from the latest raw batch.

Design decisions worth stating to the client:
* Consistency checks verify that `customer_id`, `product_id`, `store_id` exist **in the master file**. A master row that fails its own critical check (for example a product with blank category) is quarantined from the dimension, but orders that reference it still load; the dimension gets an `UNKNOWN` member so the star schema has no orphan keys. This is why `UNKNOWN` appears in the category margin (3 customers and 8 products are affected) and why gross margin for those orders is blank.
* Store S07 has no zone: it loads (Warning) with zone `UNASSIGNED`. Its revenue (85,382) is shown separately instead of guessing "South".
* `as_of` = 2 Oct 2026 (Airflow Variable `nammamart_as_of`). The files are a 1 Oct export; the wall clock would make "future date" checks depend on the day you run.

---

## Part C: Scheduling

Both DAGs: `schedule="0 9 * * 1-6"`, `start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata")`, `catchup=False`, `default_args = {retries: 2, retry_delay: 2 min, execution_timeout: 15 min}`, `max_active_runs=1`. Airflow 3.2.2 loaded both with no import errors and reports timezone `Asia/Kolkata`.

* **Cron expression:** `0 9 * * 1-6` (09:00, Monday to Saturday).
* **Time in UTC in the Airflow UI:** 03:30 UTC (IST = UTC+5:30). Verified: next runs are Sat 17 Oct, Mon 19 Oct, Tue 20 Oct at 09:00 IST = 03:30 UTC. Seen in the Airflow UI (`screenshots/03_schedule.png`): schedule `0 9 * * 1-6`, the scheduler's run for 2026-10-21 09:00 (stored as 03:30 UTC) succeeded, next run 2026-10-22 09:00. The API reports `next_dagrun_logical_date = 2026-10-22T03:30:00Z` and `start_date = 2026-09-30T18:30:00Z` (= 2026-10-01 00:00 IST). Note: the Details tab (`03b_etl_dag_details.png`) prints `Timezone: UTC` (an Airflow 3.2 display field); the DAG object's timezone is `Asia/Kolkata` and the 03:30Z run time proves the cron is evaluated in IST.
* **What if `start_date` were UTC?** Airflow would interpret `0 9 * * 1-6` in UTC, so the run fires at 09:00 UTC = 14:30 IST, five and a half hours after the 09:00 SLA and after the 09:30 stand-up. The Monday-Saturday days could also shift for runs near midnight.
* **What if the laptop or Docker is off at 09:00?** Nothing runs. With `catchup=False` the missed run is not back-filled; when Airflow starts again it only schedules the next slot. The morning report is therefore late. Fix: run Airflow on an always-on server (or cloud scheduler), and add an alert when no successful run exists by 09:15.
* **Why `catchup=False`?** The inputs are full files, not daily partitions. Back-filling the days between 1 Oct and today would re-run the same work repeatedly and flood the history. Idempotent loads would make that harmless but pointless.
* **Why retries and timeout?** Retries cover a file still being written or a short DuckDB lock; the execution timeout stops a hung task from blocking the report.

---

## Part D: Data quality dimensions

Every check has a name, dimension and severity (`include/nammamart/dq.py` for ETL, the same names in `include/sql/*.sql` for ELT; `00_dq_catalog.sql` is generated from the Python rules so both always agree). Critical = quarantined, Warning = loads and is logged. Each run writes every check to `dq_results(run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)`: 80 rows per pipeline per run.

| Dimension | Checks (file.check, severity) |
| --- | --- |
| Completeness | orders: customer_id_not_null, order_ts_not_null, delivered_has_delivery_minutes (Critical); products: category_not_null (Critical); customers: city_not_null (Warning); payments: gateway_ref_present_for_online (Warning); volume_vs_recent_max (Warning, optional) |
| Uniqueness | order_id, payment_id, customer_id, product_id, store_id unique (Critical) |
| Validity | order_status_valid, payment_mode_valid, order_ts_parses, numeric_fields_valid, discount_pct_range, payment_status_valid (Critical); order_status_casing, payment_mode_format, email_valid, phone_10_digits, pincode_6_digits, is_active_valid, zone_valid (Warning) |
| Accuracy | quantity_positive, amount_matches_formula, delivery_minutes_plausible (< 120 and > 0), mrp_positive, cost_price_positive, cost_not_above_mrp, amount_positive (Critical); capacity_positive (Warning) |
| Consistency | customer_exists, product_exists, store_exists, payment_order_exists, success_amount_matches_order (Critical); loyalty_tier_casing, unit_price_matches_mrp, payment_mode_matches_order (Warning) |
| Timeliness | order_ts_not_future, signup_date_not_future, payment_ts_not_future, launch_date_not_future (Critical); latest_order_not_stale (Warning) |

Scorecard after the full run (80 checks per pipeline): 43 PASS, 26 FAIL (Critical, rows quarantined), 11 WARN (Warning, rows loaded and logged). Warnings: 12 invalid emails, 7 short phones, 3 bad pincodes, 8 tier casings, 6 blank cities, 8+8 payment-mode formats, 2 status casings, 1 store zone, 1 store capacity, and 1 stale-data warning.

---

## Part E: Data governance

**1. Ownership.** Orders and stores: Operations (store ops head). Payments: Finance (gateway owner). Customers: CRM team. Products: Category/Catalogue team. Warehouse tables (`etl_*`, `stg_*`, `mart_*`, `dq_results`, `pipeline_runs`): the data engineering owner (the FDE team now, a named NammaMart data owner later). Changing a quality rule, for example the 120-minute delivery limit, is proposed by Operations, approved by the COO (Kavya) and Finance when it affects revenue, and recorded in Git (rule name + `dq.py`/SQL change) so there is a history.

**2. Data dictionary** (PII = personal data)
| Table.column | Type | Meaning | Source | Allowed values | PII |
| --- | --- | --- | --- | --- | --- |
| etl_fact_orders.order_id | VARCHAR (PK) | Order identifier | orders.csv | ORD + digits, unique | No |
| .order_ts / .order_date | TIMESTAMP / DATE | When the order was placed | orders.csv | not in the future | No |
| .customer_id, .store_id, .product_id | VARCHAR | Keys to the dimensions | orders.csv | exist in the master files | Indirect (customer_id) |
| .quantity | BIGINT | Units ordered | orders.csv | > 0 | No |
| .unit_price, .discount_pct, .order_amount | DOUBLE | Price, discount 0-100, amount = qty x price x (1 - disc/100) | orders.csv | amount matches formula | No |
| .payment_mode | VARCHAR | How the customer paid | orders.csv (standardised) | UPI, Card, Cash on Delivery, Wallet | No |
| .order_status | VARCHAR | Lifecycle state | orders.csv (upper-cased) | DELIVERED, CANCELLED, RETURNED | No |
| .delivery_minutes | DOUBLE | Minutes to deliver | orders.csv | 0 < x < 120; required if DELIVERED | No |
| .net_revenue | DOUBLE | order_amount if DELIVERED, else 0 | derived | >= 0 | No |
| .gross_margin | DOUBLE | net_revenue - quantity x cost_price (DELIVERED); blank if cost unknown | derived | - | No |
| .within_15_min | BOOLEAN | DELIVERED and delivery_minutes <= 15 | derived | true/false/blank | No |
| etl_dim_customer.customer_name_masked | VARCHAR | First name + last initial ("Deepa D.") | customers.csv | - | Masked PII |
| .email_masked / .phone_masked | VARCHAR | `pr***@example.com`, `98******21` | customers.csv | - | Masked PII |
| .city, .area, .pincode | VARCHAR | Customer location | customers.csv | pincode 6 digits (Warning) | Yes (location) |
| .loyalty_tier | VARCHAR | Customer tier | customers.csv (title-cased) | Bronze, Silver, Gold, Platinum, UNKNOWN | No |
| etl_dim_product.category, .cost_price, .mrp | VARCHAR, DOUBLE | Category, cost, list price | products.csv | cost <= mrp; mrp > 0 | No |
| etl_dim_store.zone | VARCHAR | North, South, East, West or UNASSIGNED | stores.csv | see left | No |

**3. Lineage.** Take `mart_daily_store_kpis` for store S03 on 2026-09-01: net_revenue 4,583.80 from 6 delivered orders. `stg_orders` identifies them: ORD000003, ORD000005, ORD000046, ORD000048, ORD000067, ORD000069. Each has `source_file = orders.csv` and `_run_id`, and the same rows exist untouched in `raw_orders` with `_loaded_at`, `_source_file`, `_run_id`. In ELT the whole path is SQL you can query. In ETL the fact has `source_file` and `_run_id`, but the cleaning happened in Python before the warehouse, so you need the quarantine files and logs for the rejected rows and the original CSV for the rest. **ELT is easier**: the raw copy is in the same database. (ORD000070, also S03 on 1 Sep, is not counted: it is rejected with `product_exists`.)

**4. Personal data.** Personal columns: `full_name`, `email`, `phone` (direct identifiers), plus `area`/`pincode` of customers and `customer_id` (indirect). In `etl_dim_customer` and `stg_customers` name is shortened to "first name + initial", email to `pr***@...`, phone to `98******21`. They are masked, not deleted, so we can still join, count and spot duplicates without exposing identity. The Indian law that applies is the Digital Personal Data Protection Act, 2023 (DPDP Act): NammaMart is a Data Fiduciary and must process personal data only for a stated purpose with consent or another lawful ground, collect only what is needed, keep it accurate, protect it with reasonable security safeguards, report breaches, not keep it longer than needed, and honour correction/erasure requests.

**5. Access control.** `raw_customers` contains unmasked customer data and the raw/quarantine tables hold rejected records (the ETL quarantine file for customers already masks name, email and phone, so it can be shared with the data steward): only the data engineering owner (and Compliance for audits). Staging: data engineers. Marts and `etl_*` reporting tables: analysts, finance, ops leaders, because they hold masked data only. `raw_customers` with unmasked phone numbers: nobody for day-to-day work; access is by exception, logged, approved by the data owner. Today everything is one DuckDB file, so this needs to be enforced by moving to a warehouse with roles (or separate files) in production.

**6. Retention.** Raw data: keep as long as the business needs an audit trail (for example 1-2 years), then archive or delete. Quarantine files: 90 days after the issue is fixed (they hold raw PII). Masked customer data: for as long as the customer relationship is active plus a legal period; then delete or anonymise. `dq_results` and `pipeline_runs` hold no PII: keep long term for trends. The periods above are proposals to agree with Compliance. A scheduled cleanup job must enforce them.

**7. Secrets.** Not in code. Slack webhook, if used, is stored in an Airflow Variable or Connection (`Variable.get("SLACK_WEBHOOK_URL")`), set in the Airflow UI/environment. A secret in a DAG file or on GitHub is readable by everyone with repo access and stays in Git history even after deletion; anyone could post to Slack or call the service. This project contains no secrets; tunable settings are Variables (`nammamart_data_dir`, `nammamart_as_of`, `nammamart_volume_check_enabled`).

**8. Audit** ("show a rejected order exactly as received, the rule it broke, and prove it is not in the CFO's revenue total"). Example: ORD000469.
* *ELT:* `SELECT * FROM raw_orders WHERE order_id = 'ORD000469'` returns the row exactly as received (quantity `0`, amount `0.0`, status `DELIVERED`, from `orders.csv`, with `_run_id`). `SELECT * FROM elt_quarantine WHERE row_key = 'ORD000469'` gives the rule: `quantity_positive`. `SELECT count(*) FROM stg_orders WHERE order_id='ORD000469' AND dq_status <> 'QUARANTINE'` returns 0, so it is in no mart.
* *ETL:* the row is in `include/quarantine/pravin/orders_failed_<run_id>.csv` with `failure_reason = quantity_positive`; `etl_rejected_rows` counts the reason; `SELECT count(*) FROM etl_fact_orders WHERE order_id='ORD000469'` returns 0.
Both prove it. ELT is simpler because the "as received" copy is in the warehouse; ETL needs the quarantine file to be kept.

**9. Change history** (an order DELIVERED on 3 Sep and RETURNED on 1 Oct). Example from the data: ORD000271 (4 Sep, 445.00, S06). After the incremental load both pipelines show it as RETURNED with `net_revenue = 0`, so September revenue changes from 1,134,962.80 to 1,130,769.10 (the 10 corrected orders remove 4,193.70). Finance wants both: the **corrected** September number for the real picture and the **as-reported** number that was sent to the CFO on 2 Oct. To keep both views: keep the raw files (ELT does this by default: both the DELIVERED row and the RETURNED row exist in `raw_orders` with their `_loaded_at`), and add a snapshot or an SCD Type 2 order-status history table (`valid_from`, `valid_to`) plus a September "as of 2 Oct" snapshot of the marts. A returns report by return date (October) is then possible without rewriting September.

---

## Part F: Observability tests

Run audit: the last task (`run_audit`, `trigger_rule="all_done"`) writes one row per run to `pipeline_runs`. Sample from the full run: ETL `rows_extracted 5372, rows_loaded 3023, rows_quarantined 165, success`; ELT `rows_extracted 5372, rows_loaded 5372, rows_quarantined 165, success`. If a task did not finish, `status` lists it (`failed: validate,...`) and the DAG run is marked failed.

How to run each experiment: `python scripts/make_experiment_data.py` creates copies under `include/data/experiments/`. In the Airflow UI set Variable `nammamart_data_dir` to `/usr/local/airflow/include/data/experiments/<folder>`, trigger the DAG, then delete the Variable to restore. The originals are never edited. Below, "Observed (data)" first came from a run of the same code outside Airflow; experiments 5, 7, 8 and 9 were then also run through the Airflow DAGs (run ids `exp5_air_etl/elt`, `exp7_...`, `exp8_...`, `exp9_...` in `pipeline_runs` and `dq_results`), with the same outcome: 5 and 8 and 9 identical in ETL and ELT. Exp 5: both pipelines quarantined 170 rows (165 + the 5 injected `ORDEXP...` rows with the reasons listed). Exp 8: `amount_matches_formula` failed 162 rows in both pipelines. Exp 9: `orders.latest_order_not_stale` = WARN in both. Exp 7 (50 orders): both runs green, no warning without the volume check; ETL and ELT differ by 9 on `payments.payment_order_exists` (2,181 vs 2,172) in this degenerate case where almost every payment is orphaned; on the normal data the two pipelines agree exactly (`scripts/compare_etl_elt.py`). Experiments 4, 6 and 10 were observed in the Airflow UI (screenshots `07`-`11`).

| # | Experiment | Expected | Observed (data) | Where I saw it | What I would change |
| --- | --- | --- | --- | --- | --- |
| 1 | Happy path | All tasks green; 5 extracts in parallel; quarantine file per file with failures | ETL: 5,212 rows in (September files), 2,308 clean orders, 165 rows quarantined (customers 8, products 10, orders 104, payments 43 distinct rows). ELT: same. Observed in Airflow (`01_etl_graph.png`, `02_elt_graph.png`): all 5 extract tasks started within 0.03 s of each other (10:20:53.875 to 10:20:53.903 UTC), so they ran in parallel; ETL run 14 s, ELT run 24 s, all tasks green | `pipeline_runs`, `dq_results`, quarantine files, task logs | Add an alert if no successful run by 09:15 |
| 2 | Re-run | Fact count unchanged; ELT raw grows | ETL fact stayed 2,308; revenue unchanged. ELT `raw_orders` grew 2,412 -> 4,824 (new `_run_id`); `stg_orders` and marts unchanged. Correct: raw is an append-only audit log | `SELECT count(*)` on fact/raw tables; `pipeline_runs` | Add a retention job for raw (Part E.6) |
| 3 | Incremental | 10 corrections update, 150 new insert, no duplicates | ETL load: inserted 150, updated 10, unchanged 2,298 -> 2,458 rows. ELT MERGE: updated 10, inserted 150 -> `stg_orders` 2,550 rows. No duplicate `order_id` in either. ORD000017, ORD000271, ORD000332 now RETURNED | task logs (`load:`, `merge_stg_orders:` lines), fact/stg tables | Run the increment only once per file name, or keep as is (idempotent) |
| 4 | Missing file | Task fails, retries, downstream blocked | Expected from the code: `extract_products` raises `FileNotFoundError: Source file not found: .../products.csv`; retries = 2 at 2-minute intervals (`up_for_retry` twice, then `failed`); `validate`/`load_raw` and later tasks become `upstream_failed`; `run_audit` still runs (all_done) and writes `status = failed: ...`. **Observed in Airflow** (`07_exp4_retrying.png`, `08_exp4_failed.png`): `products.csv` renamed, ETL triggered. `extract_products` ran 3 times (attempts at 10:24:16, 10:26:17, 10:28:18 UTC = 2-minute retry delay, 2 retries) and ended `failed`; `validate`, `quarantine`, `transform`, `load`, `reconcile`, `report` became `upstream_failed`; the 4 other extracts stayed green. `run_audit` ran and wrote `failed: extract_products,validate,...`. The first run showed the whole DAG run as green (bug, see top of this section); after the fix the run shows **Failed** (4 min 7 s) | Airflow Grid/Graph colours, task log (3 attempts), `pipeline_runs.status` | Add email/Slack on failure (bonus) |
| 5 | New bad data | 5 rows quarantined with the right reason; counted in `dq_results` | All 5 landed correctly in both pipelines: ORDEXP001 `customer_id_not_null`, ORDEXP002 `quantity_positive; amount_matches_formula`, ORDEXP003 `store_exists`, ORDEXP004 `order_status_valid`, ORDEXP005 `order_ts_not_future`. Fact stayed 2,308 (no bad order reached finance) | quarantine CSV, `etl_rejected_rows`/`elt_quarantine`, `dq_results` | none |
| 6 | Schema change | Fail loudly, nothing loaded | `extract_orders` fails: `SchemaError: orders.csv: schema changed. missing columns=['order_amount'] unexpected columns=['total']`. Nothing loaded. **Observed in Airflow** (`11_exp6_schema_change_failed.png`): both ETL and ELT runs failed at `extract_orders` (after 3 attempts), the other extracts succeeded, every later task was `upstream_failed`, `pipeline_runs` has `rows_loaded = 0` for both, and `etl_fact_orders` stayed at 2,458 rows. Without the schema check, ETL would have failed in validation and ELT would have loaded a NULL `order_amount` silently | failed task log (first failing task is `extract_orders`) | Keep the contract check in extract; alert on failure |
| 7 | Volume anomaly (50 orders) | Warn on a >90% drop | **Without** the volume check: nothing warned. Pipeline green; ELT marts dropped to 23,636 revenue (ELT rebuilds staging); ETL fact kept the old 2,308 rows (upsert), so the drop was invisible in ETL. **With** `nammamart_volume_check_enabled=true`: `orders.volume_vs_recent_max` = WARN (50 rows vs recent max 2,412) in both | `dq_results` rows `orders.volume_vs_recent_max` | Turn the check on by default and make it Critical for orders so a partial file cannot overwrite a good day |
| 8 | Distribution drift (`unit_price` x100 for S05) | Does any check catch it? | `unit_price` x100 only: **caught**: 155 S05 orders fail `amount_matches_formula` (amount no longer equals qty x price) -> quarantined; S05 revenue disappears from the marts. If amount is scaled too (`exp8b`, silent drift): **not caught as Critical**: only the Warning `unit_price_matches_mrp`; S05 revenue becomes 7.78 million, total revenue 8.84 million (about 7.8x the normal 1.13 million), category margin jumps to 85-93% | `dq_results`, `mart_daily_store_kpis`, `mart_category_margin` | Add a distribution check: store daily revenue vs trailing average (e.g. +/- 50%), and make unit_price vs mrp a Critical above a threshold |
| 9 | Freshness (latest order 30 days old) | Timeliness warning | Both pipelines: `orders.latest_order_not_stale` = WARN (1 failed). The check ignores future-dated bad rows so a corrupted 2027 date cannot hide staleness | `dq_results` | Make stale data fail the DAG before 09:30 (alert) |
| 10 | Recovery | Clear re-runs only the failed task and downstream | **Observed in Airflow** (`09_exp10_clear_downstream_dialog.png`, `10_exp10_recovered.png`): after renaming `products.csv` back, Clear on `extract_products` with Downstream showed 8 affected tasks (`extract_products` + 7 downstream). After Confirm only those 8 re-ran (start times 10:35:37 to 10:35:45 UTC) while the four successful extracts kept their original 10:30:19 start; the run ended Success and the `pipeline_runs` row for that run changed from failed to success. This works because every task reads the previous task's saved output and writes idempotently | Airflow Graph view, task start times, `pipeline_runs` | Keep every task idempotent |

Raw outputs from the local run (for reference):
```
E1 happy path rc=0 counts={'etl_fact_orders': 2308, 'raw_orders': 4824, 'stg_orders_ok': 2308, 'net_revenue_etl': 1134962.8, 'net_revenue_elt': 1134962.8}
   quarantine by file: [('customers', 8), ('orders', 107), ('payments', 45), ('products', 11)]
E2 re-run rc=0 counts={'etl_fact_orders': 2308, 'raw_orders': 7236, 'stg_orders_ok': 2308, 'net_revenue_etl': 1134962.8, 'net_revenue_elt': 1134962.8}  (ETL fact unchanged; raw_orders grew by one batch)
E3 increment rc=0 counts={'etl_fact_orders': 2458, 'raw_orders': 9808, 'stg_orders_ok': 2458, 'net_revenue_etl': 1210421.3, 'net_revenue_elt': 1210421.3}
   INFO load: fact inserted=150 updated=10 unchanged=2298 | etl_fact_orders now has 2458 rows
   INFO merge_stg_orders: increment rows applied=160 (updated=10 inserted=150); stg_orders now 2550 rows

exp5_bad_rows: rc=0
   counts={'etl_fact_orders': 2308, 'raw_orders': 9653, 'stg_orders_ok': 2308, 'net_revenue_etl': 1134962.8, 'net_revenue_elt': 1134962.8}
   quarantine rows for ORDEXP*: [('orders', 'ORDEXP001', 'customer_id_not_null'), ('orders', 'ORDEXP002', 'quantity_positive; amount_matches_formula'), ('orders', 'ORDEXP003', 'store_exists'), ('orders', 'ORDEXP004', 'order_status_valid'), ('orders', 'ORDEXP005', 'order_ts_not_future')]
   ETL quarantine files: [('orders', 'amount_matches_formula', 19), ('orders', 'customer_id_not_null', 19), ('orders', 'order_id_unique', 12), ('orders', 'quantity_positive', 11), ('orders', 'product_exists', 9), ('orders', 'delivered_has_delivery_minutes', 8), ('orders', 'order_ts_not_future', 8), ('orders', 'delivery_minutes_plausible', 6), ('orders', 'store_exists', 6), ('orders', 'order_ts_parses', 5), ('orders', 'order_status_valid', 4), ('orders', 'discount_pct_range', 3), ('orders', 'payment_mode_valid', 2), ('orders', 'order_ts_not_null', 1)]

exp6_schema_change: rc=1
   FAILED: nammamart.common.SchemaError: orders.csv: schema changed. missing columns=['order_amount'] unexpected columns=['total']

exp7_volume_50_orders: rc=0
   counts={'etl_fact_orders': 2308, 'raw_orders': 7286, 'stg_orders_ok': 48, 'net_revenue_etl': 1134962.8, 'net_revenue_elt': 23636.4}
   volume rows with the check OFF: [('nammamart_elt_pravin', 'orders.volume_vs_recent_max', 50, 0, 'PASS'), ('nammamart_etl_pravin', 'orders.volume_vs_recent_max', 50, 0, 'PASS')]
   with the check ON:            [('nammamart_elt_pravin', 'orders.volume_vs_recent_max', 50, 1, 'WARN'), ('nammamart_etl_pravin', 'orders.volume_vs_recent_max', 50, 1, 'WARN')]

exp8_drift_unit_price: rc=0
   counts={'etl_fact_orders': 2168, 'raw_orders': 9648, 'stg_orders_ok': 2168, 'net_revenue_etl': 1057168.35, 'net_revenue_elt': 1057168.35}
   category margin: [('Dairy & Bakery', 160072.6, 42372.51, 26.47), ('Fruits & Vegetables', 84188.8, 20294.35, 24.11), ('Household', 183428.95, 47131.68, 25.69), ('Personal Care', 187368.7, 41744.77, 22.28), ('Snacks & Beverages', 146184.8, 34276.05, 23.45), ('Staples', 233777.3, 58876.42, 25.18), ('UNKNOWN', 62147.2, None, None)]
   S05 store revenue in mart: []
   quarantined orders for S05 by reason: [('amount_matches_formula', 155), ('customer_id_not_null', 17), ('order_id_unique', 11), ('quantity_positive', 10), ('product_exists', 9)]

exp8b_drift_price_and_amount: rc=0
   counts={'etl_fact_orders': 2308, 'raw_orders': 9648, 'stg_orders_ok': 2308, 'net_revenue_etl': 8836613.35, 'net_revenue_elt': 8836613.35}
   category margin: [('Dairy & Bakery', 1105687.6, 980950.69, 88.72), ('Fruits & Vegetables', 456938.8, 390119.12, 85.38), ('Household', 1772678.95, 1624389.19, 91.63), ('Personal Care', 1600988.7, 1444055.58, 90.2), ('Snacks & Beverages', 1687599.8, 1563666.45, 92.66), ('Staples', 1886582.3, 1699183.22, 90.07), ('UNKNOWN', 326137.2, None, None)]
   S05 store revenue in mart: [('S05', 7779445.0)]
   quarantined orders for S05 by reason: [('customer_id_not_null', 18), ('amount_matches_formula', 15), ('order_id_unique', 12), ('quantity_positive', 10), ('product_exists', 9)]

exp9_stale_30_days: rc=0
   counts={'etl_fact_orders': 2310, 'raw_orders': 9648, 'stg_orders_ok': 2310, 'net_revenue_etl': 1136142.6, 'net_revenue_elt': 1136142.6}
   stale warnings: [('nammamart_elt_pravin', 'orders.latest_order_not_stale', 1, 'WARN'), ('nammamart_etl_pravin', 'orders.latest_order_not_stale', 1, 'WARN')]
```

### Five pillars of data observability
| Pillar | What this setup has | Gap |
| --- | --- | --- |
| Freshness | `latest_order_not_stale`, `*_ts_not_future`, `pipeline_runs.end_time` | no alert when the run is missing; no SLA at 09:15 |
| Volume | `rows_checked` per file each run, `*.volume_vs_recent_max` (optional) | off by default (Experiment 7) |
| Schema | strict column contract in every extract, failing loudly (Experiment 6) | no check for changed data types or new values in categories |
| Distribution | `unit_price_matches_mrp` (Warning), formula check | no trend check on revenue or margin by store (Experiment 8b) |
| Lineage | ELT: `_source_file`, `_run_id`, `_loaded_at` + SQL files; ETL: `source_file`, `_run_id`, quarantine files | ETL lineage stops at the CSV; no automatic lineage graph |

**Strongest pillar: schema** (and quality/validity generally): every file is checked on every run and a change stops the run. **Weakest: distribution**, which only has one Warning and can miss a consistent price/amount drift.

---

## Part G: Recommendation to Sameer (CTO)

**Recommendation: adopt ELT as the foundation, with the validated marts as the only tables finance can see. In other words: keep every raw row (ELT), but let finance read only quality-gated tables.** Run ETL's strict gate as SQL inside the warehouse, not as separate Python.

**What I observed.** I built both on the same files. They produced identical results: 2,458 clean orders, revenue 1,210,421.30, 165 rejected rows, and the same 329 payment mismatches (4 types, 204,559.55 unreconciled). So correctness is not the difference. The difference is what each approach makes easy.

* **Auditability.** ELT keeps every raw record exactly as received with its file and run. When finance asked "show a rejected order as received, the rule, and prove it is not in revenue" (ORD000469), ELT answered it with three queries in one database. ETL needs the quarantine CSV to be kept and cross-referenced. ELT also kept both the DELIVERED and the RETURNED version of the 10 corrected orders, which finance needs for September "as reported" versus "as corrected".
* **Data quality guarantees.** Both stopped every bad row: after adding 5 bad rows the fact still held 2,308 clean orders. ETL's guarantee is structural (bad rows never enter the warehouse); ELT's is a filter (`dq_status <> 'QUARANTINE'`), so a missing filter would expose bad rows. Mitigation: marts are the only layer finance can query.
* **Ease of change.** A new rule or a new question is one SQL edit and a rebuild from raw, with history replayable. In ETL, a new rule needs a code change and a re-extraction of the source files, and what was dropped earlier is gone unless the file was kept.
* **Performance at 1000x.** 1000x is about 2.4 million orders a month (about 5 million rows in all files). Pandas in memory (ETL) would need chunking or a bigger machine, while SQL transformations in a warehouse scale with the engine. DuckDB handles tens of millions of rows on one machine; beyond that move to a server warehouse and keep the same SQL.
* **Cost.** Both use open-source tools. ELT stores raw data (cheap storage, about 5x the data volume if every run appends a copy, which needs a retention rule). ETL uses less storage but more compute time and more engineering time for every change.

**Top 3 risks of this design and how to manage them**
1. **Raw PII is stored unmasked and grows on every run** (DPDP Act exposure). Mitigate: restrict raw access, keep only the latest N runs (or store only changed rows), define retention, and mask at the first staging layer.
2. **Quality gate is a filter, not a wall.** A forgotten `dq_status` filter exposes bad rows. Mitigate: expose only marts to finance, test that no mart contains a quarantined key, and show `dq_results` on the dashboard.
3. **Single-file DuckDB and a laptop scheduler.** One writer at a time and no run if the machine is off, so the 09:00 SLA is not guaranteed (Part C). Mitigate: run Airflow on an always-on server and move to a server warehouse with roles as volume or users grow; alert when no successful run exists by 09:15.

Also flagged for the client: the files do not tell us the true zone of store S07 (85k of revenue is shown as `UNASSIGNED`), 150 orders from 1 Oct show as `NO_PAYMENT` because the gateway export for 1 Oct was not provided, and 96 payments (80.9k) belong to orders that were rejected, which finance should chase.

## Bonus (all four built)

| Bonus item | Where | How it was checked |
| --- | --- | --- |
| Slack alert on failure, webhook in an Airflow Variable | `include/nammamart/alerts.py`, `on_failure_callback` in both DAGs | Variable `nammamart_slack_webhook` (never in code). Tested with a local test server: message received. Without the Variable the alert is only logged and the DAG is unaffected. |
| SCD Type 2 customer dimension (loyalty-tier history) | `include/sql/60_dim_customer_scd2.sql` (ELT: `dim_customer_scd2`); same SQL reused by the ETL load (`etl_dim_customer_scd2`) | `scripts/test_scd2.py`: 397 current rows, re-run changes nothing, then C00001 Bronze to Platinum gives 2 rows (old closed, new current) in both pipelines. |
| Store-capacity check (daily orders vs `capacity_orders_per_day`) | `include/sql/57_mart_store_capacity.sql` (ELT: `mart_store_capacity`); ETL: `etl_store_capacity` built in `report` | 432 store-days OK, 31 store-days `CAPACITY_UNKNOWN` (store S12 has capacity 0, the data issue in Part 0 row 35), 0 over capacity. Highest use is 6% of capacity. Both pipelines give the same counts. |
| Data-quality trend chart from `dq_results` | `scripts/dq_trend_chart.py`, output `answers/dq_trend.png` | Pass rate per dimension per run for ETL and ELT. The sample chart uses 3 runs on the same data, so the lines are flat; it becomes a real trend as daily runs accumulate. |

To set up the alert in Airflow: Admin > Variables > add `nammamart_slack_webhook` with a Slack incoming-webhook URL.
