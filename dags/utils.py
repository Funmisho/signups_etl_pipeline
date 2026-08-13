import logging

logger = logging.getLogger(__name__)

def notify_failure(context: dict) -> None:
    """Shared Airflow failure callback for operational alerts."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exception = context.get("exception", "No exception stack trace available.")
    logical_date_str = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")
    log_url = context["task_instance"].log_url

    logger.error(
        "\n===================================================================\n"
        "[EMAIL WOULD SEND] To: oncall@paynaija.com |\n"
        "Subject: Airflow Task Failed: %s.%s |\n"
        "Body: Task '%s' in DAG '%s' failed on logical_date %s.\n"
        "Exception: %s\n"
        "Log URL: %s\n"
        "===================================================================",
        dag_id,
        task_id,
        task_id,
        dag_id,
        logical_date_str,
        exception,
        log_url,
    )
