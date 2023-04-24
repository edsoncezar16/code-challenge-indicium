from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 4, 22),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

northwind_to_lfs = DAG(
    "northwind_to_lfs",
    default_args=default_args,
    description="A meta-DAG to manage the execution of meltano_csv-to-lfs and meltano_postgres-to-lfs",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
)

start = DummyOperator(task_id="start", dag=northwind_to_lfs)

# Add TriggerDagRunOperator for meltano_csv-to-lfs
trigger_meltano_csv_to_lfs = TriggerDagRunOperator(
    task_id="trigger_meltano_csv-to-lfs",
    trigger_dag_id="meltano_csv-to-lfs",
    dag=northwind_to_lfs,
)

# Add TriggerDagRunOperator for meltano_postgres-to-lfs
trigger_meltano_postgres_to_lfs = TriggerDagRunOperator(
    task_id="trigger_meltano_postgres-to-lfs",
    trigger_dag_id="meltano_postgres-to-lfs",
    dag=northwind_to_lfs,
)

end = DummyOperator(task_id="end", dag=northwind_to_lfs)

# Set the dependencies in northeind_to_lfs
start >> trigger_meltano_csv_to_lfs >> end
start >> trigger_meltano_postgres_to_lfs >> end
