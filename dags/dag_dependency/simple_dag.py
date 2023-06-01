import subprocess
from time import sleep

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.models import DagModel
from airflow.operators.python_operator import PythonOperator
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunDeferrableOperator,
)
import datetime

from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

# from dags.error import on_failure_pause

as_op_dataset=Dataset("dag_1_dataset")
dag_1_dataset=Dataset("dag_1_dataset")


class SparkSubmitOperatorXCom(DatabricksSubmitRunDeferrableOperator):

    def execute(self, context):
        super().execute(context)
        return self._hook._driver_status


def on_failure_pause(context):
    orm_dag = DagModel(dag_id="sample_workflow")
    orm_dag
    # orm_dag.set_is_paused(is_paused=True)


# def final_status(**kwargs):
#     for task_instance in kwargs['dag_run'].get_task_instances():
#         if task_instance.current_state() != State.SUCCESS and \
#                 task_instance.task_id != kwargs['task_instance'].task_id:
#             raise Exception("Task {} failed. Failing this DAG run".format(task_instance.task_id))


def update_task_status(**kwargs):
    recoverable_tasks = ["this_print_hello_task"]
    failed_tasks = []
    task_instances = kwargs["dag_run"].get_task_instances()

    for task_instance in task_instances:
        if (
            task_instance.current_state() != State.SUCCESS
            and task_instance.task_id != kwargs["task_instance"].task_id
        ):
            failed_tasks.append(task_instance.task_id)

    if failed_tasks:
        if failed_tasks[0] in recoverable_tasks:
            print(
                f"Failed task {failed_tasks[0]} is recoverable. Marking this as skipped. Not failing anything."
            )
            for task_instance in task_instances:
                if task_instance.task_id == failed_tasks[0]:
                    task_instance.set_state(state=State.SKIPPED)
        else:
            for task_instance in task_instances:
                if task_instance.current_state() != State.FAILED:
                    task_instance.set_state(state=State.FAILED)
            raise Exception(
                "Task {} failed. Marking all tasks failed and failing this DAG run".format(failed_tasks)
            )


def update_task_status_improved(**kwargs):
    recoverable_tasks = ["this_print_hello_task"]
    failed_tasks = []
    task_instances = kwargs["dag_run"].get_task_instances()

    for task_instance in task_instances:
        if (
            task_instance.current_state() != State.SUCCESS
            and task_instance.task_id != kwargs["task_instance"].task_id
        ):
            failed_tasks.append(task_instance)

    if failed_tasks:
        if failed_tasks[0].task_id in recoverable_tasks:
            print(
                f"Failed task {failed_tasks[0].task_id} is recoverable. Marking this as skipped. Not failing anything."
            )
            failed_tasks[0].task_id.set_state(state=State.SKIPPED)
        else:
            for task_instance in task_instances:
                task_instance.set_state(state=State.FAILED)
            raise Exception(
                "Task {} failed. Marking all tasks failed and failing this DAG run".format(failed_tasks)
            )



def final_status(**kwargs):
    failed_tasks = []
    task_instances = kwargs['dag_run'].get_task_instances()

    for task_instance in task_instances:
        if task_instance.current_state() != State.SUCCESS and \
                task_instance.task_id != kwargs['task_instance'].task_id:
            failed_tasks.append(task_instance.task_id)

    if failed_tasks:
        if failed_tasks[0] in ["this_print_hello_task"]:
            print("The task which failed is negotiable. Not failing anything.")
            for task_instance in task_instances:
                if task_instance.task_id == failed_tasks[0]:
                    task_instance.set_state(state=State.SKIPPED)
        else:
            for task_instance in task_instances:
                if task_instance.current_state() != State.FAILED:
                    task_instance.set_state(state=State.FAILED)
            raise Exception("Task {} failed. Marking all tasks failed and failing this DAG run".format(failed_tasks))


# def on_failure_pause(dag_id):
#     # return lambda x: DagModel(dag_id=dag_id).set_is_paused(is_paused=True)
#     orm_dag = DagModel(dag_id=dag_id)
#     orm_dag.set_is_paused(is_paused=True)


def fail_on_error(result: subprocess.CompletedProcess, process: str = "CLI process"):
    if result.returncode:
        print(f"Process [{process}] failed with error: {result.stderr.decode()}")
        result.check_returncode()  # fail the DAG if Azure login fails
    else:
        print(f"Process [{process}] completed with stdout: [{result.stdout.decode()}]")


def print_hello():
    cmd = "ls /Users/anuj-tw/workspace/poc/test_airflow"
    result = subprocess.run(cmd.split(" "), capture_output=True)
    print("sleeping")
    sleep(10)
    print("awake now")
    fail_on_error(result)

def print_hello1():
    cmd = "ls /Users/anuj-tw/workspace/poc/test_airflow_1"
    result = subprocess.run(cmd.split(" "), capture_output=True)
    print("sleeping")
    sleep(10)
    print("awake now")
    fail_on_error(result)


@task(task_id="this_print_hello_task")
def print_hello_task():
    cmd = "ls /Users/anuj-tw/workspace/poc/test_airflow_task"
    result = subprocess.run(cmd.split(" "), capture_output=True)
    print("sleeping")
    sleep(10)
    print("awake now")
    fail_on_error(result)


# with DAG(
#     dag_id="dag_1",
#     schedule="* */4 * * *",
#     start_date=datetime.datetime(2023, 5, 20),
#     catchup=False,
#     max_active_runs=1,
#     tags=["source"],
# ) as dag:
#
#     PythonOperator(
#         task_id="sample_task_1", python_callable=print_hello, dag=dag, outlets=[dag_1_dataset]
#     )


with DAG(
    dag_id="dag_1",
    schedule="*/1 * * * *",
    start_date=datetime.datetime(2022, 12, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sample"],
    on_failure_callback=on_failure_pause,
    default_args={
        "owner": "airflow",
        "email_on_failure": False,
        "depends_on_past": True,
        "wait_for_downstream": True,
    },
) as dag:

    PythonOperator(
        task_id="sample_task_1", python_callable=print_hello, dag=dag, outlets=[as_op_dataset]
    ) >> PythonOperator(
        task_id="sample_task_2", python_callable=print_hello, dag=dag, outlets=[as_op_dataset]#, depends_on_past=False
    ) >> PythonOperator(
        task_id="sample_task_3", python_callable=print_hello, dag=dag, outlets=[as_op_dataset]#, depends_on_past=False
    ) >> PythonOperator(
        task_id="sample_task_4", python_callable=print_hello1, dag=dag, outlets=[as_op_dataset]#, depends_on_past=False
    ) >> print_hello_task() >> PythonOperator(
    task_id='final_status',
    provide_context=True,
    retries=0,
    python_callable=final_status,
    trigger_rule=TriggerRule.ALL_DONE, # Ensures this task runs even if upstream fails
    dag=dag,
)