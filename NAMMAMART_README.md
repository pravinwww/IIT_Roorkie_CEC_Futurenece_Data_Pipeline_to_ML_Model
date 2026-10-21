# NammaMart ETL & ELT Pipelines (FDAIE Module 2)

Two Airflow DAGs (TaskFlow API) on the NammaMart dataset: `nammamart_etl_pravin` (pandas) and `nammamart_elt_pravin` (DuckDB SQL).
Both run at 09:00 IST, Mon to Sat. Written answers are in `SUBMISSION.md`; the data issues log is in `PART0_Data_Issues_Log.md`.

## Layout
- `dags/` the two DAGs (thin wrappers). `meridian_pipeline.py` is untouched.
- `include/nammamart/` Python code: `common.py`, `dq.py`, `etl.py`, `elt.py`
- `include/sql/` all SQL used by the ELT DAG (raw, staging, marts, DQ)
- `include/data/nammamart/` copy of the dataset (originals not edited); `include/data/experiments/` Part F inputs
- `answers/` business-question SQL and results for both pipelines
- `scripts/` local runner, ETL/ELT parity check, experiment and answer generators

## Run in Airflow (Astro CLI + Docker)
1. Copy these folders into your Astro project (your fork): `dags/`, `include/`, `answers/`, `scripts/`, `SUBMISSION.md`, `PART0_Data_Issues_Log.md`.
2. Add `duckdb>=1.4.0` and `pandas` to `requirements.txt` (the MERGE statement needs a recent DuckDB).
3. `astro dev start`, open the Airflow UI, unpause both DAGs, trigger each once manually.
4. Optional Variables (Admin > Variables): `nammamart_as_of`, `nammamart_volume_check`. No secrets are used.
5. Warehouse: `include/warehouse_pravin.duckdb`. Rename `pravin` in the DAG ids and file names if you want a different suffix.

## Run without Airflow (quick check)
```
python3 scripts/run_local.py both --data-dir include/data/nammamart --db /tmp/wh.duckdb --quarantine-dir /tmp/q --work-dir /tmp/w
python3 scripts/compare_etl_elt.py /tmp/wh.duckdb      # expect: ETL and ELT match
```

## Part F experiments
Inputs are in `include/data/experiments/exp4..exp9`. Point the DAG data path at the folder (env `NAMMAMART_DATA_DIR`), trigger, and record what you see in the UI. `scripts/run_experiments_local.py` reproduces experiments 1-3 and 5-9 locally. Experiments 4 and 10 need the Airflow UI.

## Screenshot checklist (only you can do these)
- Grid and graph view of each DAG, successful run
- Schedule and timezone shown on the DAG page
- Task logs of validate/quarantine and run_audit
- One failed or retried task from an experiment
- Replace every `[confirm in Airflow UI]` in SUBMISSION.md with what you observed

## Show results for screenshots
`astro dev bash` then `python scripts/show_results.py` prints dq_results, pipeline_runs and the payment reconciliation tables.

## Privacy note
`.gitignore` keeps `include/warehouse_*.duckdb` out of GitHub: the raw table inside holds unmasked customer data, which must not be published outside `include/data/`.

## Bonus items
- Slack alert: Airflow Variable `nammamart_slack_webhook` (optional)
- SCD2 proof: `python3 scripts/test_scd2.py`
- Trend chart: `python3 scripts/dq_trend_chart.py <warehouse.duckdb>`
- Capacity check: tables `mart_store_capacity` (ELT) and `etl_store_capacity` (ETL)

## Evidence already included
`screenshots/` holds Airflow UI captures from a real Airflow 3.2.2 run (graph views, schedule, experiments 4, 6, 10). If your trainer expects screenshots from your own Docker run, repeat the steps in the guide and replace them.

## Status
Verified locally: ETL and ELT outputs match, increment gives 150 inserted / 10 updated, reruns are idempotent, both DAGs parse in Airflow 3.2.2.
Also run in a real Airflow 3.2.2 scheduler (pip install, not Docker). Not done: running under Astro/Docker on your machine, and pushing to your fork.
