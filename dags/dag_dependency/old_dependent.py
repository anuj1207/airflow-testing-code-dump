import datetime

from airflow import DAG, Dataset
from airflow.decorators import dag
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator


@dag(
    dag_id="dag_a",
    start_date=datetime.datetime(2023, 5, 20),
    schedule="0 */4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["dependent"],
)
def dag_a_workflow():
    actual_task = PythonOperator(
        # .....
    )
    external_task_sensor_dag_1 = ExternalTaskSensor(
        task_id="dag_1_sensor", poke_interval=60, timeout=180, external_dag_id="dag_1",
    )
    external_task_dag_2 = ExternalTaskSensor(
        task_id="dag_2_sensor", poke_interval=60, timeout=180, external_dag_id="dag_2",
    )
    external_task_sensor_dag_3 = ExternalTaskSensor(
        task_id="dag_3_sensor", poke_interval=60, timeout=180, external_dag_id="dag_3",
    )
    [
        external_task_sensor_dag_1,
        external_task_dag_2,
        external_task_sensor_dag_3,
    ] >> actual_task
