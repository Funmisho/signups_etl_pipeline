# PayNaija Data Engineering Pipelines

A progressive Apache Airflow learning project, built one project at a time as part of a structured mentorship series. Each DAG models a realistic business problem for a fictional fintech, **PayNaija**, and is reviewed/hardened like a production system rather than a tutorial exercise.

Currently contains three independent DAGs:

- **`signups_etl_pipeline`** — daily customer signups CSV → Postgres
- **`exchange_rates_etl_pipeline`** — daily FX rate enrichment from a public API → Postgres
- **`bank_settlement_sensor_pipeline`** — waits for a late-arriving bank settlement file before processing it

---

## 1. Signups ETL Pipeline (`dags/signups_etl_pipeline.py`)

Automates what used to be a manual process: an analyst cleaning a daily `signups.csv` export in Excel and pasting it into a reporting spreadsheet for the BI team.

**Tasks:** `extract` → `clean_and_split_data` → `load_data`

- **Extract** — Verifies the incoming CSV exists and isn't empty before any processing begins.
- **Clean & Split** — Applies data quality rules and separates records into two outputs:
  - `cleaned_signups.csv` — valid rows, ready to load
  - `rejected_signups.csv` — rows with missing `signup_id`/`email` or exact duplicates, each tagged with a `rejection_reason` so nothing disappears without a trace
- **Load** — Loads clean rows into a `customer_signups` Postgres table using a run-scoped staging table (cloned from the production schema via `LIKE ... INCLUDING ALL`) and an `INSERT ... ON CONFLICT (signup_id) DO NOTHING` upsert, so retries and overlapping runs are safe.

**Reliability hardening:**
- Per-task retry tuning based on actual failure characteristics — `extract`/`clean_and_split_data` don't retry (their failures are deterministic), `load_data` retries with exponential backoff (its failures are typically transient DB/network issues).
- Structured `logging` throughout (INFO for progress, WARNING for rejected/anomalous data) instead of `print()`.
- A `try/finally` block guarantees the run-scoped staging table is dropped even if the load partially fails, preventing orphaned tables from piling up after exhausted retries.

**Design decisions worth noting:**
- **Modularity over convenience** — extract, clean, and load are separate tasks so each can fail, retry, and be tested independently.
- **No dropped rows** — every row that fails validation is preserved in a rejected file with a reason, rather than vanishing.
- **String-typed IDs everywhere** — `signup_id` is read as a string (`dtype=str`) at every point the pipeline touches a CSV, preventing pandas from silently stripping leading zeros or misinterpreting alphanumeric IDs as integers.
- **Credentials via Airflow Connections** — Postgres credentials are never hardcoded; the DAG references a `postgres_conn_id` configured in the Airflow UI.

---

## 2. Exchange Rates ETL Pipeline (`dags/exchange_rate_pipeline.py`)

Automates daily FX rate collection so PayNaija's finance team can convert transaction values across countries into a common reporting currency, replacing a manual daily lookup.

**Tasks:** `extract_raw_exchange_rates` → `transform_exchange_rates` → `load_clean_exchange_rates`

- **Extract** — Calls the [open ExchangeRate-API endpoint](https://www.exchangerate-api.com/docs/free) (no key required), validates both the HTTP response and the response body's own `result` field, and checks each target currency (NGN, GHS, KES) individually for presence and validity — logging a warning and proceeding with whatever's usable rather than hard-failing on a single missing currency. The exact, untouched JSON payload is appended to an **immutable** `raw_exchange_rates` audit table (no upsert — every fetch gets its own row), so any historical API response can be inspected later.
- **Transform** — Re-reads the raw JSON by its exact row ID (passed via XCom, avoiding any race condition with concurrent runs), independently re-derives currency validity, and reshapes the data into a long/narrow format (one row per currency) for easy filtering and future extensibility.
- **Load** — Bulk-upserts the small in-memory record list into a `clean_exchange_rates` reporting table via `psycopg2.extras.execute_values`, keyed on the composite `UNIQUE (logical_date, currency_code)`, with `ON CONFLICT ... DO UPDATE` — so a later, corrected fetch for the same day overwrites the earlier one, while the raw table still preserves every fetch that ever happened.

**Design decisions worth noting:**
- **Bronze/silver pattern** — `raw_exchange_rates` is an append-only source of truth; `transform` is a pure, re-runnable function over it (no dependency on any other task's intermediate state), and `clean_exchange_rates` is the single current-truth row per date/currency for reporting.
- **XCom sized deliberately** — the raw JSON payload is never passed through XCom (write it to Postgres instead, for audit durability); the final ~3-row cleaned record list is small enough to pass through XCom directly, skipping an unnecessary intermediate file.
- **Referential integrity** — `clean_exchange_rates.source_raw_id` is a foreign key into `raw_exchange_rates(id)`, so every reporting row is traceable back to the exact raw fetch it came from.

---

## 3. Bank Settlement Sensor Pipeline (`dags/bank_settlement_pipeline.py`)

Handles a file this team doesn't control: a daily bank settlement export dropped by Treasury Ops sometime "in the morning," with no fixed, reliable arrival time (historically anywhere from 6am to past 11am). Instead of assuming the file is already there (like `signups_etl_pipeline` does), this pipeline actively waits for it, within reason.

**Tasks:** `wait_for_settlement_file` (`FileSensor`) → `process_settlement_file`

- **Wait for file** — A `FileSensor` polls for `/tmp/settlement_file.csv` every 5 minutes (`poke_interval=300`), for up to 7 hours (`timeout=timedelta(hours=7)`), running in `mode="reschedule"` so it releases its worker slot between checks instead of blocking a slot for hours doing nothing. If the file never shows up within the window, the sensor hard-fails (`soft_fail=False`) so on-call is alerted clearly, rather than the run silently skipping.
- **Process file** — Once the sensor confirms the file exists, re-verifies its presence defensively (guarding against the narrow window between the sensor's last successful poke and the task actually opening the file) before reading and logging its contents.

**Design decisions worth noting:**
- **`reschedule` over `poke` mode** — since the wait can span hours, holding a worker slot the whole time would needlessly starve other DAGs of execution capacity.
- **Duration-based `timeout`, accepted deliberately** — Airflow's sensor `timeout` counts from actual task start, not a fixed wall-clock deadline, so a delayed scheduler start could theoretically push the cutoff later. Treated as an acceptable tradeoff here: a rare, minor delay in giving up is preferable to the complexity of building a true fixed-clock cutoff, and to the risk of false-alarming while the file is still legitimately expected.
- **Fail loud, not silent** — `soft_fail=False` ensures a genuine "file never arrived" scenario surfaces as a clear task failure (and alert), distinct from other kinds of pipeline errors.

---

## Tech stack

- Apache Airflow (TaskFlow API)
- Python (pandas, numpy, requests, psycopg2)
- PostgreSQL

## Status

🚧 Work in progress — part of a progressive Airflow learning series (8 planned projects). Completed so far: basic ETL with idempotent loading, retries/logging hardening, an API-based bronze/silver ingestion pattern, and a `FileSensor`-based pipeline that waits for a late-arriving file rather than assuming it's already there. Next up: Airflow Variables and Jinja templating.

## Project structure

```
paynaija-airflow-pipelines/
├── dags/
│   ├── signups_etl_pipeline.py
│   ├── exchange_rate_pipeline.py
│   └── bank_settlement_pipeline.py
├── data/
│   └── sample_signups.csv
├── requirements.txt
├── .gitignore
└── README.md
```