import datetime

from airflow import DAG, Dataset
from airflow.decorators import dag
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator

DAG_ID = "dag_a"

from airflow.models import DagRun

# def get_most_recent_dag_run(dt):
#     dag_runs = DagRun.find(dag_id="dag_id")
#     dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
#     if dag_runs:
#         return dag_runs[0].execution_date

as_op_dataset=Dataset("dag_1_dataset")
mas_dataset=Dataset("dag_2_dataset")
mdm_dataset=Dataset("dag_3_dataset")

dag_1_dataset=Dataset("dag_1_dataset")
dag_2_dataset=Dataset("dag_2_dataset")
dag_3_dataset=Dataset("dag_3_dataset")

def print_hello():
    return (
        f"--Placeholder -- This step should call the sample workflow in the future"
    )

@dag(
    dag_id=DAG_ID,
    default_args={
        "owner": "airflow",
        "email_on_failure": False,
        "depends_on_past": True,
        "wait_for_downstream": False,
    },
    start_date=datetime.datetime(2022, 12, 1),
    schedule=[as_op_dataset, mas_dataset, mdm_dataset],
    catchup=False,
    max_active_runs=1,
    tags=["dependent", "sample"],
)
def dag_a():

    py_op = PythonOperator(
        task_id="sample_dag", python_callable=print_hello
    )

    py_op

dag_a()