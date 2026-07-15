from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


def notify_failure(context):
    """Production adapter sends context to PagerDuty/Slack; local mode logs it."""
    task = context["task_instance"]
    print(f"ALERT dag={task.dag_id} task={task.task_id} run={context.get('run_id')}")


defaults = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
    "sla": timedelta(hours=2),
}

with DAG("rentcars_lakehouse", start_date=days_ago(1), schedule="0 * * * *", catchup=False,
         default_args=defaults, max_active_runs=1, tags=["rentcars", "data-platform"]) as dag:
    transform = BashOperator(
        task_id="transform_and_load",
        cwd="/opt/airflow/project",
        bash_command=("python -m pipeline.run --input data/raw --output data/lake "
                      "--checkpoint data/state/checkpoints.json "
                      "--watermark-days {{ var.value.get('watermark_days', 7) }}"),
    )
    compact = BashOperator(
        task_id="compact_small_files",
        cwd="/opt/airflow/project",
        bash_command="python -m pipeline.compact data/lake/events --threshold-mb 128 --apply",
    )
    transform >> compact
