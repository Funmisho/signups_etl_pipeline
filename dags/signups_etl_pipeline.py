import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
import re
from airflow.operators.python import get_current_context

# Define explicit paths for Airflow's environment
INPUT_FILE_PATH = "/tmp/signups.csv"
CLEAN_OUTPUT_PATH = "/tmp/cleaned_signups.csv"
REJECTED_OUTPUT_PATH = "/tmp/rejected_signups.csv"

# define dag using DAG decorator
@dag(
    start_date=datetime(2026, 7, 18),
    schedule="@daily",
    catchup=False,
    tags=["signups", "etl"],
)
def signups_etl_pipeline():

    @task()
    def extract(filepath: str) -> str:
        """Checks if csv file exists and isn't empty.
         Returns the filepath string for the next task.
         """

        # does the file exist?
        if not os.path.exists(filepath):
            raise FileNotFoundError(
                f"Extraction failed: File '{filepath}' does not exist."
            )
             
        # is the file empty? (i.e size is 0 bytes)
        if os.path.getsize(filepath) == 0:
            raise ValueError(
                f"Extraction failed: File '{filepath}' is empty."
            )
        
        # if it passes the check, read and return the data
        print(f"Success: File verified at '{filepath}'")
        return filepath

    @task()
    def clean_and_split_data(filepath: str) -> dict:
        """apply data quality rules.
         separates data into clean and rejected and writes both to disk.
        """
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
  
        # step 5: write to disk
        clean_data.to_csv(CLEAN_OUTPUT_PATH, index=False)
        rejected_data.to_csv(REJECTED_OUTPUT_PATH, index=False)

        print(f" Processing complete!")
        print(f"  - Clean rows saved: {len(clean_data)} to {CLEAN_OUTPUT_PATH}")
        print(f"  - Rejected rows saved: {len(rejected_data)} to {REJECTED_OUTPUT_PATH}")

        # pass the output path downstream to load task
        return {
            "clean_path" : CLEAN_OUTPUT_PATH,
            "rejected_path" : REJECTED_OUTPUT_PATH,
        }

    @task()
    def load_data(paths: dict):
        """Loads data idempotently into production using a run-specific
        staging table and an ON CONFLICT clause.
        """
        clean_file = paths["clean_path"]
        clean_df = pd.read_csv(clean_file, dtype=str)

        if clean_df.empty:
            print("No clean records to load today.")
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
                print(f"creat exact structural clone for '{staging_table}'...")
                cursor.execute(create_staging_sql)
                conn.commit()

        # now we write data to the clean, isolated staging table using append
        print(f"Staging {len(clean_df)} rows into '{staging_table}'...")
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
                print(f"Merging staging data into production table '{prod_table}'...")
                cursor.execute(merge_sql)
                print(f"Dropping isolated table '{staging_table}'...")
                cursor.execute(drop_staging_sql)
                conn.commit()

        print("Idempotent load complete!")

    # Setting up pipeline dependencies
    file_path = extract(INPUT_FILE_PATH)
    paths = clean_and_split_data(file_path)
    load_data(paths)

# Instantiate the DAG
signups_etl_pipeline()




    










