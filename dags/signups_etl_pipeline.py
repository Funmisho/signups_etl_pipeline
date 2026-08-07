import os
import re
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
from airflow.operators.python import get_current_context
from airflow.models import Variable

logger = logging.getLogger(__name__)

# define dag using DAG decorator
@dag(
    start_date=datetime(2026, 7, 18),
    schedule="@daily",
    catchup=False,
    tags=["signups", "etl"],
)
def signups_etl_pipeline():

    @task()
    def extract() -> str:
        """Checks if csv file exists and isn't empty.
         Returns the filepath string for the next task.
         """

        # Fetch path from Airflow Variable inside task execution context
        input_filepath = Variable.get("SIGNUPS_INPUT_PATH")

        # does the file exist?
        if not os.path.exists(input_filepath):
            raise FileNotFoundError(
                f"Extraction failed: File '{input_filepath}' does not exist."
            )
             
        # is the file empty? (i.e size is 0 bytes)
        if os.path.getsize(input_filepath) == 0:
            raise ValueError(
                f"Extraction failed: File '{input_filepath}' is empty."
            )
        
        # if it passes the check, read and return the data
        logger.info(f"Success: File verified at '{input_filepath}'")
        return input_filepath

    @task()
    def clean_and_split_data(filepath: str) -> dict:
        """Applies data quality rules.
         separates data into clean and rejected, and writing both to paths specified in
         Airflow Variables.
        """
        # Fetch path from Airflow Variables
        clean_output_path = Variable.get("SIGNUPS_CLEAN_PATH")
        rejected_output_path = Variable.get("REJECTED_OUTPUT_PATH")

        # read the data handed off from extract step
        data = pd.read_csv(filepath, dtype=str)

        # Step 1: clean string space safely in text fields)
        text_cols = ["full_name", "email", "phone_number"]
        data[text_cols] = data[text_cols].map(
            lambda x: x.strip() if isinstance(x, str) else x
        )

        # Step 2: Identify rejections (missing values)
        missing_id_mask = data["signup_id"].isna() | (data["signup_id"] == "")
        missing_email_mask = data["email"].isna() | (data["email"] == "")
        is_invalid = missing_id_mask | missing_email_mask

        # separate inital invalid rows
        rejected_data = data[is_invalid].copy()

        # Dynamically assign the rejection reason
        conditions = [
            missing_id_mask & missing_email_mask,
            missing_id_mask,
            missing_email_mask
        ]
        reasons = [
            "missing signup_id AND missing email",
            "missing signup_id",
            "missing email",
        ]
        rejected_data["rejection_reason"] = np.select(
            conditions, reasons, default="Unknown reason"
        )

        # Step 3: Isolate initial clean data
        clean_data = data[~is_invalid].copy()

        # convert other missing fields to NULL
        clean_data = clean_data.replace("", np.nan)

        # Step 4: Log duplicates
        # Find all duplicates in the clean dataset (keep the first one, flag all subsequent)
        duplicate_mask = clean_data.duplicated(keep="first")

        if duplicate_mask.any():
            duplicates_df = clean_data[duplicate_mask].copy()
            duplicates_df["rejection_reason"] = "Duplicate row"

            # combine missing values rejection with duplicate rejections
            rejected_data = pd.concat(
                [rejected_data, duplicates_df], ignore_index=True
            )

            # strip the duplicates out of the final clean dataframe
            clean_data = clean_data[~duplicate_mask]
  
        # step 5: write out outputs
        clean_data.to_csv(clean_output_path, index=False)
        rejected_data.to_csv(rejected_output_path, index=False)

        logger.info(
            "Processing complete! Saved %d clean rows to '%s'", 
            len(clean_data),
            clean_output_path,
        )

        if len(rejected_data) > 0:
            logger.warning(
                "%d rows rejected during data quality checks. Details written to '%s'",
                len(rejected_data),
                rejected_output_path,
            )

        # pass the output path downstream to load task
        return {
            "clean_path" : clean_output_path,
            "rejected_path" : rejected_output_path,
        }

    @task(
            retries=3,
            retry_delay=timedelta(minutes=2),
            retry_exponential_backoff=True,  # waits longer between each attempt (2m, 4m, 8m)
    )
    def load_data(paths: dict):
        """Loads data idempotently into production using a run-specific
        staging table and an ON CONFLICT clause.
        """
        clean_file = paths["clean_path"]
        clean_df = pd.read_csv(clean_file, dtype=str)

        if clean_df.empty:
            logger.warning("No clean records available to load into database today.")
            return
        
        # Fetch Airflow context to for this task to grab unique run_id
        context = get_current_context()
        run_id = context["run_id"]

        # Sanitize run_id for use in table names
        cleaned_run_id = (re.sub(r"[^a-zA-Z0-9]", "_", run_id).lower())

        prod_table = "customer_signups"
        staging_table = f"staging_{prod_table}_{cleaned_run_id}"
                
        # Initialize the hook
        pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
        engine = pg_hook.get_sqlalchemy_engine()

        # create staging table as an empty clone of production. 
        # so it inherits the exact datatypes perfectly.
        create_staging_sql = f""" 
            CREATE TABLE IF NOT EXISTS {staging_table}
            (LIKE {prod_table} INCLUDING ALL);

            -- Empty the table just incase a retry left data behind
            TRUNCATE TABLE {staging_table};
        """

        # Execute an idempotent upsert from staging to production
        # this SQL handles conflicts gracefully
        merge_sql = f"""
            INSERT INTO {prod_table} (
            signup_id, full_name, email, phone_number,
            signup_date, referral_source, plan_type
            )
            SELECT signup_id, full_name, email, phone_number,
            signup_date, referral_source, plan_type
            FROM {staging_table}
            ON CONFLICT(signup_id)
            DO NOTHING;
        """

        # clean up by removing temporary staging table
        drop_staging_sql = f"DROP TABLE IF EXISTS {staging_table};"

        # Execute table setup and data merge in a single session
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                logger.info("Initializing unique staging table '%s'...", staging_table)
                cursor.execute(create_staging_sql)
                conn.commit()

        try:
            # now we write data to the clean, isolated staging table using append
            logger.info("Staging %d clean rows into '%s'...", len(clean_df), staging_table)
            clean_df.to_sql(
                name=staging_table,
                con=engine,
                if_exists='append', # uses our pre-made schema
                index=False,
                method="multi",
                chunksize=1000,
            )

        # perform the final sync
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    logger.info("Merging staging data into production table '%s'...", prod_table)
                    cursor.execute(merge_sql)
                    conn.commit()

        finally:
                # always drop staging table (runs on success or failure of previous step)
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    logger.info("Cleaning up staging table '%s'...", staging_table)
                    cursor.execute(drop_staging_sql)
                    conn.commit()

        logger.info("Idempotent load completed successfully.")

    # Setting up pipeline dependencies
    file_path = extract()
    paths = clean_and_split_data(file_path)
    load_data(paths)

# Instantiate the DAG
signups_etl_pipeline()




    










