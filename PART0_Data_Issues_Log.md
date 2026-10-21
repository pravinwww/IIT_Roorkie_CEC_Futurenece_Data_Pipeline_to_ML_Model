# Part 0: Data Profiling and Issues Log (NammaMart)

Profiled with pandas on the 6 files in `assignment/nammamart_dataset/` (all columns read as text). Counts below are row counts found by my scripts.

## 1. Profile summary

| File | Rows | Key | Nulls / blanks | Duplicate keys |
| --- | --- | --- | --- | --- |
| orders.csv | 2,412 | order_id | customer_id 18, delivery_minutes 164, order_ts 1 (+5 unparseable text) | 12 (all exact duplicate rows) |
| payments.csv | 2,228 | payment_id | gateway_ref 301 (all Cash on Delivery, expected) | 4 (exact duplicates) |
| customers.csv | 405 | customer_id | city 6 | 5 ids repeated (rows differ in phone/area/pincode) |
| products.csv | 152 | product_id | category 3 | 2 ids repeated |
| stores.csv | 15 | store_id | zone 1 (S07) | 0 |
| orders_2026-10-01_increment.csv | 160 | order_id | none | 0 (10 ids already exist in orders = corrections, 150 new) |

Distinct values: order_status has DELIVERED 2182, CANCELLED 156, RETURNED 69 plus bad `Pending??` (3) and `delivered` (2). payment_mode has the 4 valid values plus `upi`, `UPI `, `CARD`, `cod`, `Cashh`. loyalty_tier has `BRONZE`, `bronze`, `SILVER` casing variants. zone has one blank. Delivery minutes range 8 to 1440.

Referential integrity: orders.customer_id all exist in customers (but 18 are blank); 9 product_ids and 5 store_ids are not in the masters; 8 payments point to order ids that do not exist; 27 DELIVERED orders have no payment row. Increment file: no key problems, amounts all match the formula.

## 2. Data Issues Log

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
| 13 | orders | 9 rows, e.g. ORD000986 | product_id | Not in products | Consistency | FK exists | Critical |
| 14 | orders | 5 rows, e.g. ORD001799 | store_id | Not in stores | Consistency | FK exists | Critical |
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
| 34 | stores | S07 | zone | Blank (BTM Layout, probably South) | Completeness | zone in North/South/East/West | Critical |
| 35 | stores | S12 | capacity_orders_per_day | 0 | Accuracy | capacity > 0 | Warning |
| 36 | increment | 10 ids, e.g. ORD000017, ORD000271 | order_status | Corrections: DELIVERED to RETURNED (not an error; test upsert) | Timeliness / change data | MERGE on order_id | Info |

Every file has at least one issue (orders 15, payments 6, customers 7, products 4, stores 2, increment 1 informational).

Note: the assignment mentions "test entries". I looked for obvious test patterns in ids, names and customers and found none, so the "test entries" are most likely the duplicate and invalid-status rows above. This is something to ask the client.

## 3. Three questions for the client

1. Is a negative or zero quantity a return or a data-entry error, and should it ever reduce revenue?
2. When the same customer_id appears twice with different phone/area/pincode, which row is the correct one: latest, or the one in the CRM master?
3. Should a RETURNED order (e.g. DELIVERED on 3 Sep, RETURNED on 1 Oct) reduce September revenue after the fact, or only be counted in October?

Other questions: is 120 minutes the right cut-off for impossible delivery times; can unit_price legitimately differ from product mrp; what should happen with `Pending??` orders.
