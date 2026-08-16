# PayNaija Data Engineering Pipelines

I'm using a fictional fintech, **PayNaija**, as the backdrop for a series of Airflow projects I'm building through a structured mentorship — each one starts as a business problem, gets designed before any code is written, and gets reviewed like a real PR before I move on. This repo is where that work lives.

Four DAGs so far, each one built to force a different set of decisions rather than just practice new syntax:

| DAG | What it solves | What it forced me to think about |
|---|---|---|
| `signups_etl_pipeline` | Replaces a manual daily Excel cleanup of a customer signups export | Idempotent loading, not silently dropping bad rows |
| `exchange_rates_etl_pipeline` | Pulls daily FX rates from a public API for finance reporting | Raw vs. cleaned data (bronze/silver), API failure modes, branching on anomalous rates |
| `bank_settlement_sensor_pipeline` | Waits for a settlement file that lands at an unpredictable time each morning | Sensors, worker slot cost, when to give up waiting, triggering another DAG on success |
| `reconciliation_dag` | Weekly finance reconciliation across signups, FX rates, and settlements | Cross-DAG data passing, idempotent weekly upserts, resilience to upstream DAGs failing entirely |

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

**Branching on anomalies (Project 6):** finance once had a bad GHS rate — a 40% overnight jump from a bad data point at the provider — go straight into `clean_exchange_rates` unnoticed. Now, after `transform_exchange_rates` runs, a `check_for_anomalies` task compares each currency's new rate against yesterday's *loaded* rate (a fresh query against `clean_exchange_rates`, not something carried over in memory — `transform` never had yesterday's value to begin with). Any currency that swings more than `ANOMALY_THRESHOLD_PERCENTAGE` (an Airflow Variable, 15% by default) routes the whole run to a `flagged_exchange_rates` table instead of production — same schema as the clean table, plus `previous_rate_to_usd` and `percentage_change`, so a reviewer sees the full picture without going and finding the raw data themselves.

The branching decision itself is split into two tasks on purpose: `check_for_anomalies` does the actual math and packages a decision, and a separate `route_decision` (the one wearing `@task.branch()`) just reads that decision and tells Airflow which path to take. They could've been one task, but keeping "compute" and "route" separate meant I could reason about each independently, and it's obvious from the Grid view alone which branch a given day took.

The one edge case that took real thought: what happens the very first time this DAG runs, when there's no "yesterday" row to compare against? Flagging Day 1 as anomalous by default would mean Day 2 also has no valid baseline (since nothing loaded on Day 1) — a permanent lockout where a human has to manually intervene every single day forever. Cold start is treated as normal instead, on the reasoning that you can't meaningfully call something "a 40% jump" with nothing to jump from.

**Notifications (Project 7):** flagging an anomaly used to just sit quietly in a table until someone thought to check it. Now `flag_anomalous_rates` fires a notification the moment it finishes writing to `flagged_exchange_rates` — currency, old rate, new rate, percentage change, right there in the alert instead of making someone go query for it. I'm not wiring real SMTP for this project, so it's a clearly-labeled `[EMAIL WOULD SEND]` log for now; the point was getting the trigger placement and payload right, not standing up a mail server.

This is a genuinely different kind of alert from a task failure, and it needed a different mechanism. An anomaly isn't Airflow failing at anything — it's the branch working exactly as designed. So it's a direct log call inside `flag_anomalous_rates` itself, not something routed through `on_failure_callback`.

---

## `bank_settlement_pipeline.py`

**The problem:** Treasury Ops drops a settlement file "sometime in the morning" — anywhere from 6am to past 11am, sometimes not at all. This pipeline needed to wait, but not forever, and not by blocking a worker the whole time.

A `FileSensor` polls every 5 minutes in `mode="reschedule"`, which frees the worker slot between checks instead of holding it hostage for potentially hours — `poke` mode would've been fine for a 30-second wait, but not for this. It gives up after 7 hours and fails loudly (`soft_fail=False`) rather than silently skipping, since "the file never showed up" needs to page someone, not vanish quietly.

One thing worth knowing if you're reading the timeout logic: it's duration-based (7 hours from actual task start), not a fixed wall-clock deadline. A delayed scheduler start could theoretically push the cutoff a bit later. I decided that's an acceptable tradeoff for now — false-alarming while the file is still legitimately expected felt like the worse failure mode to build around.

Once `process_settlement_file` finishes, a `TriggerDagRunOperator` kicks off `reconciliation_dag`. It sits after `process_settlement_file` in the task list, but its `trigger_rule` is set to `ALL_DONE` rather than the default `all_success` — meaning it fires whether the settlement processing succeeded *or* failed. I got this wrong the first time: I originally left it on `all_success`, which meant a missing settlement file would silently prevent reconciliation from running at all that day, with no signal to finance until some later day's successful run happened to backfill the gap. `ALL_DONE` plus `reconciliation_dag` querying `processed_settlement_files` (or finding nothing there) means a Friday failure shows up in that week's report the same afternoon, not whenever the pipeline next happens to succeed.

`TriggerDagRunOperator` also needed `logical_date="{{ logical_date }}"` explicitly — I'd assumed Airflow automatically carries the triggering DAG's logical date over to the triggered one, since that's what my design for the weekly window relied on. It doesn't; left unset, the triggered run's logical date defaults to whenever the trigger physically fires, which would have quietly broken the week calculation on any manual re-run or backfill.

---

## Operational failure alerts

Both `exchange_rates_etl_pipeline` and `bank_settlement_sensor_pipeline` now carry a shared `on_failure_callback` (`notify_failure` in `utils.py`), set once at the DAG level rather than per-task — that way a task I add next month gets covered automatically instead of me having to remember to wire it in individually. It's a different kind of alert from the anomaly one above: this fires on genuine Airflow failures (uncaught exceptions, timeouts), not on a business condition the pipeline handled correctly.

---

## Config: what's a Variable, what stays in code

File paths (`SIGNUPS_INPUT_PATH`, `SIGNUPS_CLEAN_PATH`, `SIGNUPS_REJECTED_PATH`, `SETTLEMENT_FILE_DIR`), the FX API URL, and the required currency list all live in **Airflow Variables** now, editable from the UI without a code deploy. Retry counts, table names, and sensor poke/timeout values stayed in code — those are implementation details a data engineer would change alongside other code, not something ops needs to tweak on their own.

The settlement file path is also **Jinja-templated** — `{{ var.value.SETTLEMENT_FILE_DIR }}/settlement_{{ ds }}.csv` — so each run resolves to that day's actual file (`settlement_2026-08-05.csv`) instead of pretending there's only ever one file, forever.

One rule I'm sticking to: `Variable.get()` never gets called at DAG-file top level, only inside task bodies. Calling it at the top level means it re-runs on *every scheduler parse cycle*, not just when the DAG actually executes — a real, documented cause of scheduler slowdowns in production Airflow.

---

## `reconciliation_dag.py` — the capstone

**The problem:** finance needs a weekly reconciliation report pulling together signups, FX rates (clean and flagged), and settlement completeness. It had been a placeholder that just logged "reconciliation started" since Project 7 — this is where it actually does the job.

**How it aggregates a week:** one consolidated SQL query builds a `generate_series` date backbone for the Monday-Sunday week containing `logical_date`, then `LEFT JOIN`s it against `customer_signups`, `clean_exchange_rates`, `flagged_exchange_rates`, and a new `processed_settlement_files` audit table — which didn't exist before this project either. Without it, there was no durable record of which settlement days actually succeeded, so `bank_settlement_pipeline.py`'s `process_settlement_file` now writes a row there every time it reads a file. That gap only became obvious once I tried to write the reconciliation query and realized there was nothing to query against.

**Status isn't binary.** A week can be `IN_PROGRESS`, `IN_PROGRESS_WITH_GAPS`, `IN_PROGRESS_WITH_ANOMALIES`, `PENDING_AUDIT_GAPS`, `PENDING_FX_REVIEW`, or `FINAL`. The important design call here: gaps and anomalies get surfaced in the status the moment they're detected, mid-week, not held back until Sunday. My first draft had the logic backwards — it checked "is the week over yet" before checking "are there any known problems," which meant a Tuesday settlement gap would just show as `IN_PROGRESS` (indistinguishable from a totally healthy week) until Sunday finally rolled around. Reordering those checks was a small code change but a real shift in what the report actually communicates.

**Two failure paths that needed separate answers.** Working through this project meant tracing actual failure combinations rather than just the happy path:

- *Wednesday: an FX rate gets flagged.* Nothing in `exchange_rates_etl_pipeline` triggers reconciliation directly — only `bank_settlement_sensor_pipeline` does. So the anomaly shows up in the report once Wednesday's settlement run completes and triggers reconciliation afterward, not instantly. A short, acceptable delay.
- *Friday: the settlement file never arrives.* This one exposed a real gap. With reconciliation purely trigger-driven off settlement *success*, a sensor timeout meant `process_settlement_file` never ran, the trigger never fired, and Friday's gap wouldn't appear in the report until some later day happened to succeed and retroactively notice it. On-call gets paged when the sensor times out — but finance's report stays silently stale in the meantime, which isn't the same thing as finance actually knowing.

**The fix is two triggers doing different jobs, not one.** `reconciliation_dag` now has its own independent `schedule="0 14 * * *"` — a guaranteed daily run that doesn't depend on any other DAG's health at all, even if `bank_settlement_sensor_pipeline` itself got paused or broke entirely. Layered on top, `trigger_reconciliation`'s `trigger_rule` is `ALL_DONE` instead of the default `all_success`, so it also fires immediately when settlement processing finishes — success or failure — giving faster feedback than waiting for the 2pm backstop when things are working normally. The two can both fire within minutes of each other some days; since everything is upserted on `week_start_date`, running the same aggregation twice just recomputes the same result, not a duplicate.

---

## Looking back at the whole arc

Eight projects, four DAGs, one system. A few things that carried through the whole thing rather than being specific to any one project:

**Idempotency was the actual throughline.** Every load in this repo — signups, clean FX rates, flagged FX rates, settlement audit records, weekly reports — is built to be safely re-run. That wasn't a Project 1 lesson I outgrew; it's the same question asked again in each new context: what's the natural key, and what happens if this exact task runs twice?

**Every DAG assumes its neighbors can fail.** Retries only get added where a failure is actually likely to be transient. Missing currencies don't kill a whole day's FX load. A missing settlement file doesn't silently corrupt the reconciliation report — it gets reported as missing, honestly, same day. None of this was designed in one pass; most of it came from being asked "what happens if X fails at this exact point" until an actual gap showed up.

**Config, not just code, needed a boundary.** Deciding what belongs in an Airflow Variable versus what stays hardcoded turned out to be its own real design skill — not "externalize everything" and not "externalize nothing," but a genuine judgment call about who needs to change what, and how often.

**The hardest bugs were never syntax.** The typos got caught fast. The real mistakes were things like assuming `TriggerDagRunOperator` propagates `logical_date` automatically, or a `CASE` statement that technically ran but quietly communicated the wrong thing to finance for days at a time. Those only surface by tracing through specific failure scenarios end-to-end, not by reading code top to bottom.

---

## Tech stack

Apache Airflow (TaskFlow API), Python (pandas, numpy, requests, psycopg2), PostgreSQL.

## Status

8-project mentorship series — complete. Four DAGs working together: modular idempotent ETL, retry/logging hardening, a bronze/silver API ingestion pattern, a sensor-based wait pipeline, Airflow Variables + Jinja templating, branching on anomalous rates, cross-DAG triggering with separate business-alert and operational-failure notification paths, and a capstone weekly reconciliation pipeline with a hybrid fast-path/backstop trigger architecture resilient to upstream DAGs failing outright.

## Project structure

```
paynaija-airflow-pipelines/
├── dags/
│   ├── signups_etl_pipeline.py
│   ├── exchange_rate_pipeline.py
│   ├── bank_settlement_pipeline.py
│   └── reconciliation_dag.py
├── utils.py
├── data/
│   └── sample_signups.csv
├── requirements.txt
├── .gitignore
└── README.md
```