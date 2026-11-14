"""Production-style Airflow DAG wrapping the self-healing runner.

Deploy by copying this file into your Airflow dags/ folder with the `healer`
package installed (`pip install /path/to/this/repo`). Airflow imports are
lazy so the repo's test suite runs without Airflow installed.
"""
from datetime import datetime
from pathlib import Path

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # Airflow not installed (local dev / CI)
    DAG = None
    PythonOperator = None


def _run(**context):
    from healer.config import PipelineConfig
    from healer.dataset import generate_orders  # stand-in for a real extract
    from healer.heal import SelfHealingRunner
    from healer.pipeline import OrdersPipeline

    config = PipelineConfig(
        warehouse_path=Path("/var/data/warehouse/orders.db"),
        reports_dir=Path("/var/data/reports"),
    )
    pipeline = OrdersPipeline(config)
    source = generate_orders(n=config.expected_daily_rows)  # replace with a real extract
    result = SelfHealingRunner(pipeline, source).run(source)

    # Push the outcome to XCom so downstream tasks / alerts can branch on it.
    context["ti"].xcom_push(key="healing_status", value=result.status)
    context["ti"].xcom_push(key="attempts", value=result.attempts)
    if result.status == "escalated":
        raise RuntimeError(
            f"pipeline escalated after {result.attempts} attempts; see incidents.jsonl"
        )


if DAG is not None:
    with DAG(
        dag_id="self_healing_orders",
        description="Daily orders load with detect -> diagnose -> heal self-remediation",
        schedule="@daily",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["self-healing", "data-quality"],
    ) as dag:
        PythonOperator(
            task_id="run_self_healing_pipeline",
            python_callable=_run,
        )
