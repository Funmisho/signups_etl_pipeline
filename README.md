# PayNaija Signups ETL Pipeline

An Apache Airflow DAG that automates the daily process of turning a raw customer signups CSV export into clean, validated records in a Postgres reporting table — replacing what used to be a manual, error-prone spreadsheet workflow.

## Business context

PayNaija's product team exports a daily `signups.csv` from the internal admin tool. Previously, an analyst manually cleaned this file in Excel and pasted it into a reporting spreadsheet for the BI team. This pipeline automates that process end-to-end, with an emphasis on **never silently losing or corrupting data**.

## What it does

1. **Extract** — Verifies the incoming CSV exists and isn't empty before any processing begins.
2. **Clean & Split** — Applies data quality rules and separates records into two outputs:
   - `cleaned_signups.csv` — valid rows, ready to load
   - `rejected_signups.csv` — rows with missing `signup_id`/`email` or exact duplicates, each tagged with a `rejection_reason` so nothing disappears without a trace
3. **Load** — Loads clean rows into a `customer_signups` Postgres table using a run-scoped staging table (cloned from the production schema via `LIKE ... INCLUDING ALL`) and an `INSERT ... ON CONFLICT (signup_id) DO NOTHING` upsert, so retries and overlapping runs are safe.

## Design decisions worth noting

- **Modularity over convenience** — extract, clean, and load are separate tasks so each can fail, retry, and be tested independently.
- **No dropped rows** — every row that fails validation is preserved in a rejected file with a reason, rather than vanishing.
- **String-typed IDs everywhere** — `signup_id` is read as a string (`dtype=str`) at every point the pipeline touches a CSV, to prevent pandas from silently stripping leading zeros or misinterpreting alphanumeric IDs as integers.
- **Idempotent loads** — a per-run staging table (named using the Airflow `run_id`) avoids collisions between concurrent or retried runs, and the schema is cloned directly from production so type mismatches are caught rather than silently coerced.
- **Credentials via Airflow Connections** — Postgres credentials are never hardcoded; the DAG references a `postgres_conn_id` configured in the Airflow UI.

## Tech stack

- Apache Airflow (TaskFlow API)
- Python (pandas, numpy)
- PostgreSQL

## Status

🚧 Work in progress — part of a progressive Airflow learning series. Current stage: basic ETL with idempotent loading (scheduling, retries, and logging polish coming next).

## Project structure

```
signups-etl-pipeline/
├── dags/
│   └── signups_etl_pipeline.py
├── data/
│   └── sample_signups.csv
├── requirements.txt
├── .gitignore
└── README.md
```