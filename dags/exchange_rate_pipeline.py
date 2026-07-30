import json
import logging
from datetime import datetime
from typing import Any, Dict, List
import requests
from airflow.decorators import task, dag
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values


logger = logging.getLogger(__name__)

EXCHANGE_RATE_API_URL = "https://open.er-api.com/v6/latest/USD"
REQUIRED_CURRENCIES = ["NGN", "GHS", "KES"]

@dag(
    dag_id="exchange_rates_etl_pipeline",
    start_date=datetime(2026, 7, 26),
    schedule="@daily",
    catchup=False,
    tags=["finance", "fx", "api"],
)
def exchange_rates_etl_pipeline():

    @task()
    def extract_raw_exchange_rates() -> dict:
        """Fetches raw exchange rates from API, performs semantic validation,

        and appends the exact JSON response into an immutable audit table in Postgres.

        Returns:
            dict: Contains 'raw_id' (exact DB row ID) and 'logical_date'
            string
                  to eliminate race conditions in downstream tasks.
        """
        context = get_current_context()
        logical_date_str = context["logical_date"].strftime("%Y-%m-%d")

        logger.info("Fetching exchange rate for execution date: %s", logical_date_str)

        # HTTP Network Fetch
        response = requests.get(EXCHANGE_RATE_API_URL, timeout=10)
        response.raise_for_status()

        # Safe JSON parsing
        try:
            raw_json_data = response.json()
        except ValueError as e:
            logger.error("Failed to decode JSON. Response text: %s", response.text[:200])
            raise ValueError(f"API returned 200 OK, but payload is not valid JSON: {e}")

        # Semantic Validation 
        if raw_json_data.get("result") != "success":
            raise ValueError(
                f"API returned 200 OK, but reported error: {raw_json_data.get('error-type')}"
            )

        rates = raw_json_data.get("rates", {}) 
        missing_currencies = []
        invalid_currencies = []
        valid_rates = {}

        # Explicitly inspect each target currency to catch partial payload drops
        for currency in REQUIRED_CURRENCIES:
            if currency not in rates:
                missing_currencies.append(currency)
            else:
                rate_val = rates[currency]
                if not isinstance(rate_val, (int, float)) or rate_val <= 0:
                    invalid_currencies.append((currency, rate_val))
                else:
                    valid_rates[currency] = float(rate_val)

        # Flag partial data anomalies prominently in logs without crashing execution
        if missing_currencies or invalid_currencies:
            logger.warning(
                "PARTIAL DATA WARNING for logical date %s!\n"
                "Missing currencies: %s\n"
                "Invalid rate values: %s\n"
                "Proceeding with available rates: %s\n",
                logical_date_str,
                missing_currencies if missing_currencies else "None",
                invalid_currencies if invalid_currencies else "None",
                list(valid_rates.keys()),
            )

        # Hard stop only if ALL target currencies fail
        if not valid_rates:
            raise ValueError(
                f"Extraction failed: None of the target currencies {REQUIRED_CURRENCIES} "
                f"were present or valid in the API response."
            )

        # Preserve Untouched Payload in Postgres Raw Landing Table (Audit Trail)
        pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")

        create_landing_table_sql = """
            CREATE TABLE IF NOT EXISTS raw_exchange_rates (
                id SERIAL PRIMARY KEY,
                logical_date DATE NOT NULL,
                fetched_at TIMESTAMP NOT NULL,
                source_url VARCHAR(255) NOT NULL,
                raw_response JSONB NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_raw_rates_logical_date
            ON raw_exchange_rates (logical_date);
        """

        insert_raw_sql = """
            INSERT INTO raw_exchange_rates (logical_date, fetched_at, source_url, raw_response)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """

        values = (
            logical_date_str,
            datetime.utcnow(), 
            EXCHANGE_RATE_API_URL, 
            json.dumps(raw_json_data),
        )

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_landing_table_sql)
                cursor.execute(insert_raw_sql, values,)
                inserted_id = cursor.fetchone()[0]
                conn.commit()

        logger.info(
            "Successfully recorded raw API response [Row ID: %d] for %s",
            inserted_id,
            logical_date_str,
        )

        # Pass exact record ID reference to eliminate downstream via Xcom
        return {"raw_id": inserted_id, "logical_date": logical_date_str}

    @task()
    def transform_exchange_rates(extract_metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Reads raw JSON payload from 'raw_exchange_rates' by exact row ID, 
        extracts and normalizes target currencies into a tall/narrow structure.

        Args:
            extract_metadata (dict): Contains 'raw_id' (int) and 'logical_date'
            (str) from extract.

        Returns:
            list[dict]: List of normalized rate records ready for loading.
        """
        raw_id = extract_metadata["raw_id"]
        logical_date_str = extract_metadata["logical_date"]

        logger.info("Starting transformation for raw_id: %d (Date: %s)", raw_id, logical_date_str)

        # Fetch extract raw JSON payload using primary keys
        pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
        select_raw_sql = "SELECT raw_response FROM raw_exchange_rates WHERE id = %s;"

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_raw_sql, (raw_id,))
                result = cursor.fetchone()
                if not result:
                    raise ValueError(f"No raw payload found in 'raw_exchange_rates' for id: {raw_id}")

                # Handles both DB JSONB types and raw JSON string fallbacks safely
                raw_payload = result[0]
                raw_json_data = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload

        # Extract rates from dictionary
        rates = raw_json_data.get("rates", {})
        if not isinstance(rates, dict):
            raise KeyError(f"Raw payload for id {raw_id} is missing a valid 'rates' dictionary")

        cleaned_records = []
        missing_or_invalid = []

        # Shape into tall/narrow format (1 row per valid currency)
        for currency in REQUIRED_CURRENCIES:
            rate_val = rates.get(currency)

            # Validate it returns a positive number
            if isinstance(rate_val, (int, float)) and rate_val > 0:
                cleaned_records.append({
                    "logical_date": logical_date_str,
                    "currency_code": currency,
                    "rate_to_usd": float(rate_val),
                    "source_raw_id": raw_id
                })
            else:
                missing_or_invalid.append((currency, rate_val))

        # Log warnings for missing currencies without failing execution
        if missing_or_invalid:
            logger.warning(
                "Transformation warning for raw_id %d: Skipped missing/invalid currencies: %s",
                raw_id,
                missing_or_invalid
            )

        # Hard failure only if transformation yields 0 usable records
        if not cleaned_records:
            raise ValueError(
                f"Transformation failed for raw_id {raw_id}: None of {REQUIRED_CURRENCIES} "
                f"could be parsed into valid rates."
            )

        logger.info(
            "Successfully transformed %d rates for raw_id %d: %s",
            len(cleaned_records),
            raw_id,
            [r["currency_code"] for r in cleaned_records]
        )

        # pass list of dicts directly in memory via Xcom
        return cleaned_records

    @task()
    def load_clean_exchange_rates(clean_records: List[Dict[str, Any]]) -> None:
        """Loads normalized currency records into 'clean_exchange_rates'.

        Executes an atomic, idempotent upsert using psycopg2 execute_values
        targeting the composite key ON CONFLICT (logical_date, currency_code).
        """
        if not clean_records:
            logger.warning(
                "No clean records recieved in load_clean_exchange_rates task. Skipping load."
            )
            return

        logger.info(
            "Preparing atomic bulk upsert for %d clean rate record(s)...",
            len(clean_records),
        )

        pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")

        # NOTE: DDL included here for local bootstrapping convenience.
        # In enterprise production, table creation is managed via migration scripts (e.g. Alembic).
        create_clean_table_sql = """
            CREATE TABLE IF NOT EXISTS clean_exchange_rates (
                id SERIAL PRIMARY KEY,
                logical_date DATE NOT NULL,
                currency_code VARCHAR(3) NOT NULL,
                rate_to_usd NUMERIC(18, 6) NOT NULL,
                source_raw_id INT NOT NULL REFERENCES raw_exchange_rates(id),
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_date_currency UNIQUE(logical_date, currency_code)   
            );

            CREATE INDEX IF NOT EXISTS idx_clean_rates_date
            ON clean_exchange_rates (logical_date);
        """

        # execute_values dynamically formats (%s, %s, %s, %s) for all rows into a single multi-row INSERT
        upsert_sql = """
            INSERT INTO clean_exchange_rates (
                logical_date,
                currency_code,
                rate_to_usd,
                source_raw_id
            )
            VALUES %s
            ON CONFLICT (logical_date, currency_code)
            DO UPDATE SET
                rate_to_usd = EXCLUDED.rate_to_usd,
                source_raw_id = EXCLUDED.source_raw_id,
                created_at = CURRENT_TIMESTAMP
        """

        # Transform list of dicts into tuple parameter list expected by execute_values
        records_to_insert = [
            (
                rec["logical_date"],
                rec["currency_code"],
                rec["rate_to_usd"],
                rec["source_raw_id"],
            )
            for rec in clean_records
        ]

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_clean_table_sql)
                execute_values(cursor, upsert_sql, records_to_insert)
                conn.commit()

        logger.info(
            "Successfully upserted %d record(s) into 'clean_exchange_rates'.",
            len(clean_records),
        )

    # setting up pipeline dependencies
    raw_meta = extract_raw_exchange_rates()
    cleaned = transform_exchange_rates(raw_meta)
    load_clean_exchange_rates(cleaned)

# Instantiate the DAG
exchange_rates_etl_pipeline()   
