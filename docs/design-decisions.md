# Design decisions and debugging notes

This document holds the longer reasoning behind the PayNaija Data Platform: why certain design choices were made, what tradeoffs were considered and rejected, and what actually broke the first time each pipeline ran against a real database. The main [README](../README.md) covers what the system does. This covers why it works the way it does.

## Cross-cutting design decisions

**Idempotency is the actual throughline.** Every load in this repo, signups, clean exchange rates, flagged exchange rates, settlement audit records, and weekly reports, is built to be safely re-run. It is the same question asked in a new context each time: what is the natural key, and what happens if this exact task runs twice. `signups_etl_pipeline` answers it with a run-scoped staging table and `ON CONFLICT (signup_id) DO NOTHING`. `exchange_rates_etl_pipeline` answers it differently for two tables in the same DAG: `raw_exchange_rates` is append-only, since every fetch is a distinct historical event worth keeping, while `clean_exchange_rates` upserts on `(logical_date, currency_code)`, since it represents current known state and a same-day correction should overwrite. `reconciliation_dag` upserts on `week_start_date`, so recomputing mid-week or twice within minutes is a no-op, not a duplicate.

**Config has a real boundary, not a reflex.** File paths, the FX API URL, the required currency list, and the anomaly threshold live in Airflow Variables, editable from the UI without a code deploy. Retry counts, table names, and sensor timing values stayed in code, since those are implementation details a data engineer would change alongside other code, not something operations needs to tweak independently. `Variable.get()` is never called at DAG file top level, only inside task bodies, since a top-level call re-executes on every scheduler parse cycle, not just when the DAG runs, which is a documented cause of scheduler slowdown in production Airflow.

**Two kinds of alert, two mechanisms.** An anomaly is a business condition the pipeline handled correctly, so it gets a direct notification from inside the task where it happened (`flag_anomalous_rates`). A genuine Airflow failure, an uncaught exception or a sensor timeout, gets a shared `on_failure_callback` (`notify_failure`, in `utils.py`) set once at the DAG level, so a task added later is covered automatically rather than depending on someone remembering to wire it in per task.

**Every DAG assumes its neighbors can fail.** A missing currency does not kill a whole day's FX load, it logs what is missing and proceeds with what is valid. A missing settlement file does not silently corrupt the reconciliation report, it gets reported as missing, the same day, via a hybrid trigger described below. None of this was designed in one pass. Most of it came from tracing a specific failure scenario end to end until a real gap showed up.

## Per-pipeline reasoning

### signups_etl_pipeline

The load is idempotent. Every run builds a run-scoped staging table cloned from `customer_signups` using `LIKE ... INCLUDING ALL`, so a type mismatch gets caught by Postgres instead of silently coerced by pandas, and merges into production with:

```sql
INSERT INTO customer_signups (
    signup_id, full_name, email, phone_number,
    signup_date, referral_source, plan_type
)
SELECT signup_id, full_name, email, phone_number,
       signup_date, referral_source, plan_type
FROM staging_customer_signups_<run_id>
ON CONFLICT (signup_id) DO NOTHING;
```

A `try/finally` block guarantees the staging table gets dropped even if the merge fails partway through, so a bad retry does not leave orphaned tables behind.

Rejected rows never disappear. Missing `signup_id`, missing `email`, and duplicate rows all land in `rejected_signups.csv` with a `rejection_reason` column, so if finance asks why yesterday's signup count looks low, there is an actual answer.

`signup_id` is read as a string at every point this pipeline touches a CSV. An earlier draft let pandas infer the type and it silently turned `00123` into `123`.

Retries are tuned per task rather than applied as a blanket setting. `extract` and `clean_and_split_data` do not retry, because their failures are deterministic and retrying just delays the same outcome. `load_data` retries with exponential backoff, because its failures are usually a transient database issue.

### exchange_rates_etl_pipeline

The raw API response and the cleaned version live in two separate tables. `raw_exchange_rates` is append only, every fetch gets its own row, nothing is ever updated or deleted, so a question like "what did the API actually return on a given day" always has an answer. `transform_exchange_rates` reads that raw JSON back by its exact row id rather than by date, since two runs on the same day would otherwise race each other.

A missing currency is treated as a warning, not a hard failure. If GHS is temporarily absent from the API response but NGN and KES came through, the pipeline still loads what it has and logs exactly what is missing, rather than losing all three currencies over one gap.

Branching handles anomalies. After a real incident where a bad GHS rate, a 40 percent overnight jump from a bad data point at the provider, went straight into the reporting table unnoticed, `check_for_anomalies` now compares each new rate against yesterday's loaded rate before anything is written. Any currency that swings more than `ANOMALY_THRESHOLD_PERCENTAGE` (an Airflow Variable, 15 percent by default) routes the whole run to `flagged_exchange_rates` instead of production, along with the previous rate and the percentage change, so a reviewer sees the full picture without a second query.

The branch computation and the branch decision are split into two tasks on purpose. `check_for_anomalies` does the math and packages a decision. `route_decision`, the task actually decorated with `@task.branch()`, just reads that decision and tells Airflow which path to take. This made each part easier to reason about on its own, and it means the Airflow Grid view shows exactly which path a given day took without digging into logs.

The first run of this DAG has no "yesterday" to compare against. Treating that cold start as anomalous by default would mean day two also has no valid baseline, since nothing loaded on day one, which creates a permanent lockout. Cold start is treated as normal instead.

An anomaly triggers a direct alert from inside `flag_anomalous_rates`, separate from the operational failure callback. An anomaly is not Airflow failing at anything, it is the branch working exactly as designed, so it needed its own notification path rather than being folded into the generic failure alert.

### bank_settlement_sensor_pipeline

The sensor polls every five minutes in `mode="reschedule"`, which releases its worker slot between checks instead of holding it for potentially hours. It gives up after seven hours and fails loudly rather than silently skipping, since a missing file needs to page someone, not vanish quietly.

The settlement file path is Jinja templated: `{{ var.value.SETTLEMENT_FILE_DIR }}/settlement_{{ ds }}.csv`, so each run resolves to that day's actual file rather than assuming there is only ever one file.

Once `process_settlement_file` reads the file, it writes a row to `processed_settlement_files`, a durable audit table recording the settlement date, the filepath, and the row count. This table did not exist in the original design. It only became necessary once `reconciliation_dag` needed a real way to know which days actually succeeded.

`trigger_reconciliation` fires `reconciliation_dag` with `trigger_rule=TriggerRule.ALL_DONE` rather than the default `all_success`, so it runs whether settlement processing succeeded or failed. The first version used the default trigger rule, which meant a missing settlement file silently prevented reconciliation from running at all that day, with no signal to finance until some later successful run happened to catch the gap retroactively.

The trigger also explicitly passes `logical_date="{{ logical_date }}"`. Airflow does not automatically propagate a triggering DAG's logical date to the DAG it triggers. Left unset, the triggered run's logical date defaults to whenever the trigger physically fires, which would have quietly broken the weekly window calculation on any manual re-run or backfill.

### reconciliation_dag

One consolidated SQL query builds a date backbone for the Monday through Sunday week containing the run's logical date, using `generate_series`, then left joins it against `customer_signups`, `clean_exchange_rates`, `flagged_exchange_rates`, and `processed_settlement_files`.

The report status is not binary. A week can be `IN_PROGRESS`, `IN_PROGRESS_WITH_GAPS`, `IN_PROGRESS_WITH_ANOMALIES`, `PENDING_AUDIT_GAPS`, `PENDING_FX_REVIEW`, or `FINAL`. Gaps and anomalies surface in the status as soon as they are detected, mid-week, rather than being held back until Sunday. An earlier draft checked whether the week was over before checking whether anything was wrong, which meant a Tuesday settlement gap looked identical to a fully healthy week until Sunday finally arrived. Reordering those checks changed what the report actually communicated, not just how it was written.

Since this DAG owns the only query that touches all four upstream tables, it also owns making sure they exist. `build_weekly_reconciliation` runs `CREATE TABLE IF NOT EXISTS` for all four dependency tables plus its own `weekly_reconciliation_reports` table, in an order that respects the foreign key from `clean_exchange_rates` and `flagged_exchange_rates` back to `raw_exchange_rates`. Relying on each upstream DAG's own table creation meant reconciliation would crash on a fresh database, or the first time a branch like `flag_anomalous_rates` had genuinely never been taken.

This DAG has two ways of running, on purpose. A fast path fires the moment `bank_settlement_sensor_pipeline` finishes, success or failure. An independent daily schedule at 14:00 acts as a backstop that does not depend on any other DAG's health at all, so even if `bank_settlement_sensor_pipeline` itself were paused or broken, reconciliation would still run and could report zero settlements processed that day, rather than staying silently frozen on stale data. The two can fire within minutes of each other on a normal day. Since everything upserts on `week_start_date`, running the same aggregation twice just recomputes the same result rather than duplicating anything.

## What testing found

Design review catches a lot. Every item below only surfaced by actually running these DAGs against a Postgres instance for the first time. None of them were visible from reading the code.

- **A length mismatch in `signups_etl_pipeline`.** A rejection-reason column was computed against the full dataset with `np.select`, then assigned into a DataFrame already sliced down to a subset of it. Fixed by computing the reason column on the full DataFrame first, then slicing.
- **Three separate "table does not exist" failures** (`customer_signups`, `clean_exchange_rates`, `flagged_exchange_rates`), each a table that only got created inside a task that does not always run. Fixed by making the tasks that depend on a table responsible for guaranteeing it exists, not just the task that happens to write to it first on the happy path.
- **A pandas and SQLAlchemy version incompatibility**, not a bug in this codebase at all. `to_sql(method="multi")` breaks on certain pandas 2.2 plus, SQLAlchemy 1.4 pairings with a confusing error about an Engine or Connection object having no `cursor` attribute. Fixed by switching that insert to `psycopg2.extras.execute_values`, the same pattern already used elsewhere in this repo.
- **A Jinja path mismatch.** A test settlement file's name did not exactly match the rendered `{{ ds }}` path. A `FileSensor` matches on an exact literal path, so a close filename is treated the same as no file at all.
- **A foreign key ordering issue** in `reconciliation_dag`'s table bootstrap, where `raw_exchange_rates` needed to be created before `clean_exchange_rates` and `flagged_exchange_rates`, since both reference it.