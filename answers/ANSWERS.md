
## Answers from the ETL pipeline

### Q1 Daily net revenue by zone and by store (DELIVERED only, after discount)

```sql
SELECT order_date, zone, store_id, store_name, net_revenue FROM etl_daily_zone_revenue ORDER BY order_date, zone, store_id;
```

| order_date          | zone       | store_id   | store_name             |   net_revenue |
|:--------------------|:-----------|:-----------|:-----------------------|--------------:|
| 2026-09-01 00:00:00 | East       | S08        | NammaMart Whitefield   |       2984.7  |
| 2026-09-01 00:00:00 | East       | S09        | NammaMart Indiranagar  |       2945.4  |
| 2026-09-01 00:00:00 | East       | S10        | NammaMart Marathahalli |       2141.6  |
| 2026-09-01 00:00:00 | East       | S11        | NammaMart HSR Layout   |       2121.15 |
| 2026-09-01 00:00:00 | North      | S01        | NammaMart Hebbal       |       2247.8  |
| 2026-09-01 00:00:00 | North      | S02        | NammaMart Yelahanka    |       2385.55 |
| 2026-09-01 00:00:00 | North      | S03        | NammaMart RT Nagar     |       4583.8  |
| 2026-09-01 00:00:00 | South      | S04        | NammaMart Jayanagar    |       1103.3  |
| 2026-09-01 00:00:00 | South      | S05        | NammaMart JP Nagar     |        803    |
| 2026-09-01 00:00:00 | South      | S06        | NammaMart Banashankari |       2812    |
| 2026-09-01 00:00:00 | UNASSIGNED | S07        | NammaMart BTM Layout   |       5844.75 |
| 2026-09-01 00:00:00 | West       | S12        | NammaMart Rajajinagar  |        300    |

_...448 more rows (460 total)_

```sql
SELECT order_date, zone, round(sum(net_revenue), 2) AS net_revenue FROM etl_daily_zone_revenue GROUP BY ALL ORDER BY 1, 2;
```

| order_date          | zone       |   net_revenue |
|:--------------------|:-----------|--------------:|
| 2026-09-01 00:00:00 | East       |      10192.9  |
| 2026-09-01 00:00:00 | North      |       9217.15 |
| 2026-09-01 00:00:00 | South      |       4718.3  |
| 2026-09-01 00:00:00 | UNASSIGNED |       5844.75 |
| 2026-09-01 00:00:00 | West       |       6563.65 |
| 2026-09-02 00:00:00 | East       |       8737.9  |
| 2026-09-02 00:00:00 | North      |       3573.8  |
| 2026-09-02 00:00:00 | South      |      14038    |
| 2026-09-02 00:00:00 | UNASSIGNED |        824.1  |
| 2026-09-02 00:00:00 | West       |      10797.6  |
| 2026-09-03 00:00:00 | East       |      12058.4  |
| 2026-09-03 00:00:00 | North      |      13959.1  |

_...143 more rows (155 total)_

### Q2 Revenue and gross margin by product category

```sql
SELECT * FROM etl_category_margin ORDER BY category;
```

| category            |   net_revenue |   gross_margin |   margin_pct |
|:--------------------|--------------:|---------------:|-------------:|
| Dairy & Bakery      |      179569   |        47633   |        26.53 |
| Fruits & Vegetables |       93604.2 |        22205.4 |        23.72 |
| Household           |      211727   |        54352.5 |        25.67 |
| Personal Care       |      210889   |        47054.6 |        22.31 |
| Snacks & Beverages  |      174701   |        41370.8 |        23.68 |
| Staples             |      268583   |        67143.1 |        25    |
| UNKNOWN             |       71348.1 |          nan   |       nan    |

### Q3 Average delivery time per store and % of delivered orders within 15 minutes

```sql
SELECT store_id, round(avg(delivery_minutes), 2) AS avg_delivery_minutes,
       round(100.0 * avg(within_15_min::INT), 2) AS pct_within_15_min
FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 1;
```

| store_id   |   avg_delivery_minutes |   pct_within_15_min |
|:-----------|-----------------------:|--------------------:|
| S01        |                  20.47 |               32.21 |
| S02        |                  20.93 |               31.33 |
| S03        |                  20.65 |               29.37 |
| S04        |                  21.31 |               27.54 |
| S05        |                  21.19 |               29.58 |
| S06        |                  22.52 |               21.57 |
| S07        |                  21.13 |               32.67 |
| S08        |                  20.61 |               35.37 |
| S09        |                  21.13 |               26.47 |
| S10        |                  22.09 |               26    |
| S11        |                  20.88 |               28.13 |
| S12        |                  21.86 |               24.58 |

_...3 more rows (15 total)_

### Q4 Revenue by payment mode, and the total amount that does not reconcile with the gateway

```sql
SELECT payment_mode, round(sum(net_revenue), 2) AS net_revenue FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 1;
```

| payment_mode     |   net_revenue |
|:-----------------|--------------:|
| Card             |        251882 |
| Cash on Delivery |        181668 |
| UPI              |        638752 |
| Wallet           |        138118 |

```sql
SELECT mismatch_type, count(*) AS mismatches, round(sum(unreconciled_amount), 2) AS unreconciled_amount FROM etl_payment_reconciliation GROUP BY ROLLUP(1) ORDER BY 1;
```

| mismatch_type               |   mismatches |   unreconciled_amount |
|:----------------------------|-------------:|----------------------:|
| AMOUNT_MISMATCH             |           13 |                 760   |
| NO_PAYMENT                  |          178 |               97128.6 |
| PAYMENT_FAILED              |           42 |               25722.2 |
| PAYMENT_WITHOUT_CLEAN_ORDER |           96 |               80948.8 |
| nan                         |          329 |              204560   |

### Q5 Top 10 customers by spend with loyalty tier (masked contact data only)

```sql
SELECT c.customer_id, c.customer_name_masked, c.loyalty_tier, c.email_masked, c.phone_masked, round(sum(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id) WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY total_spend DESC LIMIT 10;
```

| customer_id   | customer_name_masked   | loyalty_tier   | email_masked      | phone_masked   |   total_spend |
|:--------------|:-----------------------|:---------------|:------------------|:---------------|--------------:|
| C00136        | Sanjay B.              | Bronze         | sa***@example.com | 96******88     |       8738.1  |
| C00115        | Sanjay K.              | Bronze         | sa***@example.com | 96******85     |       8037.7  |
| C00283        | Nikhil R.              | Bronze         | ni***@example.com | 91******43     |       7934.6  |
| C00236        | Sneha N.               | Platinum       | sn***@example.com | 90******01     |       7831.8  |
| C00310        | Kiran P.               | Bronze         | ki***@example     | 91******35     |       7258.95 |
| C00265        | Vikram H.              | Gold           | vi***@example.com | 91******69     |       7151.3  |
| C00071        | Mahesh P.              | Silver         | ma***@example.com | 91******42     |       6933.3  |
| C00338        | Bhavana I.             | Silver         | bh***@example.com | 94******05     |       6848.55 |
| C00357        | Arjun N.               | Bronze         | ar***@example.com | 93*****12      |       6637.9  |
| C00209        | Kavya P.               | Platinum       | ka***@example.com | 98******97     |       6519.4  |

### Q6 Rows rejected today, by file and reason (latest run)

```sql
SELECT source_file, failure_reason, rows_rejected FROM etl_rejected_rows
WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_pravin' ORDER BY start_time DESC LIMIT 1)
ORDER BY source_file, rows_rejected DESC;
```

| source_file   | failure_reason                 |   rows_rejected |
|:--------------|:-------------------------------|----------------:|
| customers     | customer_id_unique             |               5 |
| customers     | signup_date_not_future         |               3 |
| orders        | amount_matches_formula         |              18 |
| orders        | customer_id_not_null           |              18 |
| orders        | order_id_unique                |              12 |
| orders        | quantity_positive              |              10 |
| orders        | product_exists                 |               9 |
| orders        | delivered_has_delivery_minutes |               8 |
| orders        | order_ts_not_future            |               7 |
| orders        | delivery_minutes_plausible     |               6 |
| orders        | order_ts_parses                |               5 |
| orders        | store_exists                   |               5 |

_...14 more rows (26 total)_


## Answers from the ELT pipeline

### Q1 Daily net revenue by zone and by store (DELIVERED only, after discount)

```sql
SELECT order_date, zone, store_id, store_name, net_revenue FROM mart_daily_store_kpis WHERE delivered_orders > 0 ORDER BY order_date, zone, store_id;
```

| order_date          | zone       | store_id   | store_name             |   net_revenue |
|:--------------------|:-----------|:-----------|:-----------------------|--------------:|
| 2026-09-01 00:00:00 | East       | S08        | NammaMart Whitefield   |       2984.7  |
| 2026-09-01 00:00:00 | East       | S09        | NammaMart Indiranagar  |       2945.4  |
| 2026-09-01 00:00:00 | East       | S10        | NammaMart Marathahalli |       2141.6  |
| 2026-09-01 00:00:00 | East       | S11        | NammaMart HSR Layout   |       2121.15 |
| 2026-09-01 00:00:00 | North      | S01        | NammaMart Hebbal       |       2247.8  |
| 2026-09-01 00:00:00 | North      | S02        | NammaMart Yelahanka    |       2385.55 |
| 2026-09-01 00:00:00 | North      | S03        | NammaMart RT Nagar     |       4583.8  |
| 2026-09-01 00:00:00 | South      | S04        | NammaMart Jayanagar    |       1103.3  |
| 2026-09-01 00:00:00 | South      | S05        | NammaMart JP Nagar     |        803    |
| 2026-09-01 00:00:00 | South      | S06        | NammaMart Banashankari |       2812    |
| 2026-09-01 00:00:00 | UNASSIGNED | S07        | NammaMart BTM Layout   |       5844.75 |
| 2026-09-01 00:00:00 | West       | S12        | NammaMart Rajajinagar  |        300    |

_...448 more rows (460 total)_

```sql
SELECT order_date, zone, round(sum(net_revenue), 2) AS net_revenue FROM mart_daily_store_kpis GROUP BY ALL ORDER BY 1, 2;
```

| order_date          | zone       |   net_revenue |
|:--------------------|:-----------|--------------:|
| 2026-09-01 00:00:00 | East       |      10192.9  |
| 2026-09-01 00:00:00 | North      |       9217.15 |
| 2026-09-01 00:00:00 | South      |       4718.3  |
| 2026-09-01 00:00:00 | UNASSIGNED |       5844.75 |
| 2026-09-01 00:00:00 | West       |       6563.65 |
| 2026-09-02 00:00:00 | East       |       8737.9  |
| 2026-09-02 00:00:00 | North      |       3573.8  |
| 2026-09-02 00:00:00 | South      |      14038    |
| 2026-09-02 00:00:00 | UNASSIGNED |        824.1  |
| 2026-09-02 00:00:00 | West       |      10797.6  |
| 2026-09-03 00:00:00 | East       |      12058.4  |
| 2026-09-03 00:00:00 | North      |      13959.1  |

_...143 more rows (155 total)_

### Q2 Revenue and gross margin by product category

```sql
SELECT * FROM mart_category_margin ORDER BY category;
```

| category            |   net_revenue |   gross_margin |   margin_pct |
|:--------------------|--------------:|---------------:|-------------:|
| Dairy & Bakery      |      179569   |        47633   |        26.53 |
| Fruits & Vegetables |       93604.2 |        22205.4 |        23.72 |
| Household           |      211727   |        54352.5 |        25.67 |
| Personal Care       |      210889   |        47054.6 |        22.31 |
| Snacks & Beverages  |      174701   |        41370.8 |        23.68 |
| Staples             |      268583   |        67143.1 |        25    |
| UNKNOWN             |       71348.1 |          nan   |       nan    |

### Q3 Average delivery time per store and % of delivered orders within 15 minutes

```sql
SELECT store_id, avg_delivery_minutes, pct_within_15_min FROM mart_delivery_sla ORDER BY 1;
```

| store_id   |   avg_delivery_minutes |   pct_within_15_min |
|:-----------|-----------------------:|--------------------:|
| S01        |                  20.47 |               32.21 |
| S02        |                  20.93 |               31.33 |
| S03        |                  20.65 |               29.37 |
| S04        |                  21.31 |               27.54 |
| S05        |                  21.19 |               29.58 |
| S06        |                  22.52 |               21.57 |
| S07        |                  21.13 |               32.67 |
| S08        |                  20.61 |               35.37 |
| S09        |                  21.13 |               26.47 |
| S10        |                  22.09 |               26    |
| S11        |                  20.88 |               28.13 |
| S12        |                  21.86 |               24.58 |

_...3 more rows (15 total)_

### Q4 Revenue by payment mode, and the total amount that does not reconcile with the gateway

```sql
SELECT * FROM mart_payment_mode_revenue ORDER BY 1;
```

| payment_mode     |   delivered_orders |   net_revenue |
|:-----------------|-------------------:|--------------:|
| Card             |                457 |        251882 |
| Cash on Delivery |                318 |        181668 |
| UPI              |               1205 |        638752 |
| Wallet           |                246 |        138118 |

```sql
SELECT mismatch_type, count(*) AS mismatches, round(sum(unreconciled_amount), 2) AS unreconciled_amount FROM mart_payment_reconciliation GROUP BY ROLLUP(1) ORDER BY 1;
```

| mismatch_type               |   mismatches |   unreconciled_amount |
|:----------------------------|-------------:|----------------------:|
| AMOUNT_MISMATCH             |           13 |                 760   |
| NO_PAYMENT                  |          178 |               97128.6 |
| PAYMENT_FAILED              |           42 |               25722.2 |
| PAYMENT_WITHOUT_CLEAN_ORDER |           96 |               80948.8 |
| nan                         |          329 |              204560   |

### Q5 Top 10 customers by spend with loyalty tier (masked contact data only)

```sql
SELECT rank, customer_id, customer_name_masked, loyalty_tier, email_masked, phone_masked, total_spend FROM mart_top_customers ORDER BY rank;
```

|   rank | customer_id   | customer_name_masked   | loyalty_tier   | email_masked      | phone_masked   |   total_spend |
|-------:|:--------------|:-----------------------|:---------------|:------------------|:---------------|--------------:|
|      1 | C00136        | Sanjay B.              | Bronze         | sa***@example.com | 96******88     |       8738.1  |
|      2 | C00115        | Sanjay K.              | Bronze         | sa***@example.com | 96******85     |       8037.7  |
|      3 | C00283        | Nikhil R.              | Bronze         | ni***@example.com | 91******43     |       7934.6  |
|      4 | C00236        | Sneha N.               | Platinum       | sn***@example.com | 90******01     |       7831.8  |
|      5 | C00310        | Kiran P.               | Bronze         | ki***@example     | 91******35     |       7258.95 |
|      6 | C00265        | Vikram H.              | Gold           | vi***@example.com | 91******69     |       7151.3  |
|      7 | C00071        | Mahesh P.              | Silver         | ma***@example.com | 91******42     |       6933.3  |
|      8 | C00338        | Bhavana I.             | Silver         | bh***@example.com | 94******05     |       6848.55 |
|      9 | C00357        | Arjun N.               | Bronze         | ar***@example.com | 93*****12      |       6637.9  |
|     10 | C00209        | Kavya P.               | Platinum       | ka***@example.com | 98******97     |       6519.4  |

### Q6 Rows rejected today, by file and reason (latest run)

```sql
SELECT source_file, failure_reason, rows_rejected FROM mart_rejected_rows
WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_elt_pravin' ORDER BY start_time DESC LIMIT 1)
ORDER BY source_file, rows_rejected DESC;
```

| source_file   | failure_reason                 |   rows_rejected |
|:--------------|:-------------------------------|----------------:|
| customers     | customer_id_unique             |               5 |
| customers     | signup_date_not_future         |               3 |
| orders        | customer_id_not_null           |              18 |
| orders        | amount_matches_formula         |              18 |
| orders        | order_id_unique                |              12 |
| orders        | quantity_positive              |              10 |
| orders        | product_exists                 |               9 |
| orders        | delivered_has_delivery_minutes |               8 |
| orders        | order_ts_not_future            |               7 |
| orders        | delivery_minutes_plausible     |               6 |
| orders        | store_exists                   |               5 |
| orders        | order_ts_parses                |               5 |

_...14 more rows (26 total)_

