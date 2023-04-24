from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 4, 22),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

indicium_code_challenge = DAG(
    "indicium-code-challenge",
    default_args=default_args,
    description="A meta-DAG to manage the execution of meltano generated DAGs",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
)

start = DummyOperator(
    task_id="start",
    dag=indicium_code_challenge,
)

trigger_meltano_csv_to_lfs = TriggerDagRunOperator(
    task_id="trigger_meltano_csv-to-lfs",
    trigger_dag_id="meltano_csv-to-lfs",
    dag=indicium_code_challenge,
)

trigger_meltano_postgres_to_lfs = TriggerDagRunOperator(
    task_id="trigger_meltano_postgres-to-lfs",
    trigger_dag_id="meltano_postgres-to-lfs",
    dag=indicium_code_challenge,
)

wait_for_meltano_csv_to_lfs = ExternalTaskSensor(
    task_id="wait_for_meltano_csv-to-lfs",
    external_dag_id="meltano_csv-to-lfs",
    external_task_id=None,  # waits for the entire DAG to complete
    allowed_states=["success"],
    execution_delta=None,
    dag=indicium_code_challenge,
)

wait_for_meltano_postgres_to_lfs = ExternalTaskSensor(
    task_id="wait_for_meltano_postgres-to-lfs",
    external_dag_id="meltano_postgres-to-lfs",
    external_task_id=None,  # waits for the entire DAG to complete
    allowed_states=["success"],
    execution_delta=None,
    dag=indicium_code_challenge,
)

trigger_meltano_lfs_to_analytics = TriggerDagRunOperator(
    task_id="trigger_meltano_lfs-to-analytics",
    trigger_dag_id="meltano_lfs-to-analytics",
    dag=indicium_code_challenge,
)

end = DummyOperator(
    task_id="end",
    dag=indicium_code_challenge,
)

# Set dependencies
(
    start
    >> [trigger_meltano_csv_to_lfs, trigger_meltano_postgres_to_lfs]
    >> [wait_for_meltano_csv_to_lfs, wait_for_meltano_postgres_to_lfs]
    >> trigger_meltano_lfs_to_analytics
    >> end
)
