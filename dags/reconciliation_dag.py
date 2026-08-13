import logging
from datetime import datetime

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)


@dag(
    dag_id="reconciliation_dag",
    start_date=datetime(2026, 8, 1),
    schedule=None,  # Driven explicitly by TriggerDagRunOperator
    catchup=False,
    tags=["finance", "reconciliation"],
)
def reconciliation_dag():

    @task()
    def run_reconciliation() -> None:
        """Processes weekly finance reconciliation across settlement and FX rates."""
        logger.info(
            " [RECONCILIATION] Trigger received successfully. Starting weekly reconciliation job..."
        )

    run_reconciliation()


reconciliation_dag()