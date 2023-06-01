import subprocess

from airflow import DAG, Dataset
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import datetime

mdm_dataset=Dataset("dag_3_dataset")

with DAG(
    dag_id="dag_3",
    schedule="*/1 * * * *",
    start_date=datetime.datetime(2022, 12, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sample"],
) as dag:
    def fail_on_error(result: subprocess.CompletedProcess, process: str = "CLI process"):
        if result.returncode:
            print(f"Process [{process}] failed with error: {result.stderr.decode()}")
            result.check_returncode()  # fail the DAG if Azure login fails
        else:
            print(f"Process [{process}] completed with stdout: [{result.stdout.decode()}]")


    def print_hello():
        cmd = "ls /Users/anuj-tw/workspace/poc/test_airflow_2"
        result = subprocess.run(cmd.split(" "), capture_output=True)
        fail_on_error(result)

    PythonOperator(
        task_id="sample_dag", python_callable=print_hello, dag=dag, outlets=[mdm_dataset]
    )