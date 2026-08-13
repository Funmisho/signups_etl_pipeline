import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule

from utils import notify_failure

logger = logging.getLogger(__name__)

# Template combining the Airflow Variable and the execution logical date {{ ds }}
TEMPLATED_SETTLEMENT_PATH = (
    "{{ var.value.SETTLEMENT_FILE_DIR }}/settlement_{{ ds }}.csv"
)

@dag(
    dag_id="bank_settlement_sensor_pipeline",
    start_date=datetime(2026, 8, 4),
    schedule="0 6 * * *", # Starts daily at 6:00 AM UTC
    catchup=False,
    default_args={"on_failure_callback": notify_failure},
    tags=["finance", "treasury", "sensors"],
) 

def bank_settlement_sensor_pipeline():
    # Sensor Task: Wait for file arrival
    wait_for_settlement_file = FileSensor(
        task_id="wait_for_settlement_file",
        filepath=TEMPLATED_SETTLEMENT_PATH,
        poke_interval=300, # Check every 300 seconds,
        timeout=timedelta(hours=7), # 7 hours cutoff duration
        mode="reschedule", # releases worker slots back to pool while sleeping
        soft_fail=False, # Fail tasks loudly to trigger alerts
    )

    # Downstream Task: Read and log file contents once confirmed present
    @task()
    def process_settlement_file(filepath: str) -> None:
        """Reads and logs the arrived settlement file once the sensor confirms its prescence."""
        logger.info("Starting processing for settlement file at: %s", filepath)

        if not os.path.exists(filepath):
            raise FileNotFoundError(
                f"Processing Failed: Sensor marked success, but file was missing at {filepath}"
            )

        with open(filepath, "r", encoding="utf-8") as f:
            lines = f.readlines()

        logger.info(
            "Settlement file successfully read. Total rows: %d", len(lines)
        )

        # Log first few sample lines if available
        if lines:
            logger.info("Sample preview:\n%s", "".join(lines[:5]))

    # Cross-DAG Trigger: Kicks off reconciliation_dag upon successful settlement processing
    trigger_reconciliation = TriggerDagRunOperator(
        task_id="trigger_reconciliation_dag",
        trigger_dag_id="reconciliation_dag",
        reset_dag_run=True, # Clears & re-runs existing DAG run if re-triggered for same date
        wait_for_completion=False, # Fire-and-forget; doesn't block worker waiting for completion
    )

    # Setting dependency, passing TEMPLATED_SETTLEMENT_PATH to the @task function lets Airflow
    # render the Jinja string before executing the task body.
    # Dependency Chain: Sensor -> Process -> Trigger
    (
        wait_for_settlement_file
        >> process_settlement_file(TEMPLATED_SETTLEMENT_PATH)
        >> trigger_reconciliation
    )


dag_instance = bank_settlement_sensor_pipeline() 