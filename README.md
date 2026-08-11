# PayNaija Data Engineering Pipelines

I'm using a fictional fintech, **PayNaija**, as the backdrop for a series of Airflow projects I'm building through a structured mentorship — each one starts as a business problem, gets designed before any code is written, and gets reviewed like a real PR before I move on. This repo is where that work lives.

Three DAGs so far, each one built to force a different set of decisions rather than just practice new syntax:

| DAG | What it solves | What it forced me to think about |
|---|---|---|
| `signups_etl_pipeline` | Replaces a manual daily Excel cleanup of a customer signups export | Idempotent loading, not silently dropping bad rows |
| `exchange_rates_etl_pipeline` | Pulls daily FX rates from a public API for finance reporting | Raw vs. cleaned data (bronze/silver), API failure modes, branching on anomalous rates |
| `bank_settlement_sensor_pipeline` | Waits for a settlement file that lands at an unpredictable time each morning | Sensors, worker slot cost, when to give up waiting |

---

## `signups_etl_pipeline.py`

**The problem:** an analyst was manually cleaning a daily `signups.csv` export in Excel and pasting it into a spreadsheet the BI team read from. Slow, and every "quick fix" to a bad row was invisible to everyone else.

**How it's structured:** `extract → clean_and_split_data → load_data`, kept as three separate tasks on purpose — if a transformation bug shows up later, I don't want to be forced to re-touch the extract logic (or re-hit an external system) just to fix it.

The part I spent the most time getting right wasn't the cleaning logic — it was making the load idempotent. Every run builds a run-scoped staging table (cloned from `customer_signups` via `LIKE ... INCLUDING ALL`, so type mismatches get caught by Postgres instead of silently coerced by pandas), and only merges into production with `ON CONFLICT (signup_id) DO NOTHING`. A `try/finally` around that block makes sure the staging table gets dropped even if the merge fails partway — otherwise a bad retry leaves orphaned tables behind forever.

Rejected rows (missing `signup_id`, missing `email`, exact duplicates) never just disappear — they land in `rejected_signups.csv` with a `rejection_reason` column, so if finance ever asks "why did we only get 340 signups yesterday," there's an actual answer instead of a shrug.

`signup_id` is read as a string (`dtype=str`) at every single point this pipeline touches a CSV — I got burned by this once in an earlier draft, where pandas quietly turned `00123` into `123`.

Retries are tuned per task rather than applied blanket: `extract` and `clean_and_split_data` don't retry, since their failures are deterministic and retrying just delays the same failure. `load_data` retries with exponential backoff, since its failures are usually a transient DB hiccup.

---

## `exchange_rate_pipeline.py`

**The problem:** finance needs daily NGN/GHS/KES → USD rates, currently looked up manually.

This one's built around a pattern I hadn't used before this project: keep the *raw* API response and the *cleaned* version in two separate tables. `raw_exchange_rates` is append-only — every fetch gets its own row, nothing is ever updated or deleted, so if an auditor asks "what did the API actually say on July 27th," that question is always answerable. `transform_exchange_rates` reads that raw JSON back out by its exact row ID (not by date — two runs on the same day would otherwise race) and reshapes it into `clean_exchange_rates`, one row per currency, upserted on `(logical_date, currency_code)` so a same-day correction from the API overwrites cleanly.

`extract_raw_exchange_rates` treats a missing currency as a warning, not a hard failure — if `GHS` is temporarily absent from the API response but `NGN` and `KES` came through fine, the pipeline still loads what it has and logs exactly what's missing, rather than losing all three currencies over one gap.

`clean_exchange_rates.source_raw_id` is a foreign key back into `raw_exchange_rates`, so every reporting row is traceable to the exact fetch it came from — not just "some run that day."

**Branching on anomalies:** finance once had a bad GHS rate — a 40% overnight jump from a bad data point at the provider — go straight into `clean_exchange_rates` unnoticed. Now, after `transform_exchange_rates` runs, a `check_for_anomalies` task compares each currency's new rate against yesterday's *loaded* rate (a fresh query against `clean_exchange_rates`, not something carried over in memory — `transform` never had yesterday's value to begin with). Any currency that swings more than `ANOMALY_THRESHOLD_PERCENTAGE` (an Airflow Variable, 15% by default) routes the whole run to a `flagged_exchange_rates` table instead of production — same schema as the clean table, plus `previous_rate_to_usd` and `percentage_change`, so a reviewer sees the full picture without going and finding the raw data themselves.

The branching decision itself is split into two tasks on purpose: `check_for_anomalies` does the actual math and packages a decision, and a separate `route_decision` (the one wearing `@task.branch()`) just reads that decision and tells Airflow which path to take. They could've been one task, but keeping "compute" and "route" separate meant I could reason about each independently, and it's obvious from the Grid view alone which branch a given day took.

The one edge case that took real thought: what happens the very first time this DAG runs, when there's no "yesterday" row to compare against? Flagging Day 1 as anomalous by default would mean Day 2 also has no valid baseline (since nothing loaded on Day 1) — a permanent lockout where a human has to manually intervene every single day forever. Cold start is treated as normal instead, on the reasoning that you can't meaningfully call something "a 40% jump" with nothing to jump from.

---

## `bank_settlement_pipeline.py`

**The problem:** Treasury Ops drops a settlement file "sometime in the morning" — anywhere from 6am to past 11am, sometimes not at all. This pipeline needed to wait, but not forever, and not by blocking a worker the whole time.

A `FileSensor` polls every 5 minutes in `mode="reschedule"`, which frees the worker slot between checks instead of holding it hostage for potentially hours — `poke` mode would've been fine for a 30-second wait, but not for this. It gives up after 7 hours and fails loudly (`soft_fail=False`) rather than silently skipping, since "the file never showed up" needs to page someone, not vanish quietly.

One thing worth knowing if you're reading the timeout logic: it's duration-based (7 hours from actual task start), not a fixed wall-clock deadline. A delayed scheduler start could theoretically push the cutoff a bit later. I decided that's an acceptable tradeoff for now — false-alarming while the file is still legitimately expected felt like the worse failure mode to build around.

---

## Config: what's a Variable, what stays in code

File paths (`SIGNUPS_INPUT_PATH`, `SIGNUPS_CLEAN_PATH`, `SIGNUPS_REJECTED_PATH`, `SETTLEMENT_FILE_DIR`), the FX API URL, and the required currency list all live in **Airflow Variables** now, editable from the UI without a code deploy. Retry counts, table names, and sensor poke/timeout values stayed in code — those are implementation details a data engineer would change alongside other code, not something ops needs to tweak on their own.

The settlement file path is also **Jinja-templated** — `{{ var.value.SETTLEMENT_FILE_DIR }}/settlement_{{ ds }}.csv` — so each run resolves to that day's actual file (`settlement_2026-08-05.csv`) instead of pretending there's only ever one file, forever.

One rule I'm sticking to: `Variable.get()` never gets called at DAG-file top level, only inside task bodies. Calling it at the top level means it re-runs on *every scheduler parse cycle*, not just when the DAG actually executes — a real, documented cause of scheduler slowdowns in production Airflow.

---

## Tech stack

Apache Airflow (TaskFlow API), Python (pandas, numpy, requests, psycopg2), PostgreSQL.

## Status

8-project mentorship series. Done: modular idempotent ETL, retry/logging hardening, a bronze/silver API ingestion pattern, a sensor-based wait pipeline, Airflow Variables + Jinja templating, and branching (the exchange rate pipeline now routes suspicious rate swings to a review table instead of loading them). Next: triggering one DAG from another, plus callbacks and email notifications — giving `flagged_exchange_rates` an actual reason to alert someone.

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