import datetime
import logging
import subprocess
from time import sleep
from typing import Dict, List

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunDeferrableOperator,
)
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

# from utils.error_handler import on_failure_pause
# from utils.azure import azure_login, delete_dir, dir_exists
# from utils.config_parser import ConfigParser
# from utils.databricks_job_json_generator import DatabricksJobJsonGenerator
# from utils.datasets import as_op_dataset

DAG_ID = "as_op_b_t_s_workflow"
DELTA_V_R_TASK_ID = "delta_v_r"
CLEAN_AND_SAM_TASK_ID = "clean_and_sam_5_s"
FF_AND_SAM_TASK_ID = "ff_and_sam_1_m"
PROCESSED_VERSIONS_LOGGER_TASK_ID = "processed_versions_logger"
DELETE_CHECKPOINT_DIR_TASK_ID = "delete_checkpoint_dir"
UPDATE_STATUS_TASK_ID = "update_task_status"

logger = logging.getLogger(__name__)
# config_parser = ConfigParser(Variable.get("ENVIRONMENT_IDENTIFIER"), "/opt/airflow/dags/datalake.conf")


def get_spark_delta_conf() -> Dict[str, str]:
    conf = {
        "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
        "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact": "true",
        "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true",
        "spark.databricks.delta.schema.autoMerge.enabled": "true",
        "spark.databricks.delta.formatCheck.enabled": "false",
        "spark.databricks.delta.merge.repartitionBeforeWrite.enabled": "true",
        "spark.databricks.adaptive.autoOptimizeShuffle.enabled": "true",
        "spark.databricks.delta.autoCompact.minNumFiles": "6",
        "spark.cleaner.referenceTracking.cleanCheckpoints": "true",
    }
    return conf


# def get_databricks_job_runs_submit_json(entrypoint: str, description: str, params: List[str]):
#     generator = DatabricksJobJsonGenerator(config_parser)
#
#     libraries = []
#     min_workers = 6
#     max_workers = 10
#     cores_per_worker = 8
#     total_cores = min_workers * cores_per_worker  # todo: may be change to max_workers
#     aqe_factor = 3
#     shuffle_partitions = total_cores * aqe_factor
#
#     databricks_runs_submit_job_json = generator.generate_job(
#         params,
#         Variable.get("wheel_path"),
#         description,
#         entrypoint,
#         libraries,
#         extra_spark_conf=get_spark_delta_conf(),
#         min_workers=min_workers,
#         max_workers=max_workers,
#         shuffle_partitions=shuffle_partitions,
#         instance_pool_id=Variable.get("OCTA_CORE_POOL_ID"),
#     )
#
#     return databricks_runs_submit_job_json

def fail_on_error(result: subprocess.CompletedProcess, process: str = "CLI process"):
    if result.returncode:
        print(f"Process [{process}] failed with error: {result.stderr.decode()}")
        result.check_returncode()  # fail the DAG if Azure login fails
    else:
        print(f"Process [{process}] completed with stdout: [{result.stdout.decode()}]")


def print_hello(appender: str, sleep_time:int = 10):
    cmd = "ls /Users/anuj-tw/workspace/poc/test_airflow"+appender
    result = subprocess.run(cmd.split(" "), capture_output=True)
    print("sleeping")
    sleep(sleep_time)
    print("awake now")
    fail_on_error(result)


def get_delta_rec_job_json():
    print_hello("", sleep_time=5)
    # entrypoint = "bronze_delta_v_r"
    # description = "B T S Delta V Rec"
    # params = config_parser.get_parameters_for_job("common", "delta_rec")
    # return get_databricks_job_runs_submit_json(entrypoint, description, params)


def get_clean_and_sam_5_s_job_json():
    print_hello("_1", sleep_time=5)
    # entrypoint = "b_t_s_unfilled_as_op"
    # description = "B T S Clean and Sam 5 s pipeline"
    # params = config_parser.get_parameters_for_job("as_op", "b_t_s.clean_sample")
    # return get_databricks_job_runs_submit_json(entrypoint, description, params)


def get_ff_and_sam_1_m_job_json():
    print_hello("_2", sleep_time=20)
    # entrypoint = "b_t_s_forward_fill_as_op"
    # description = "B T S FF and Sam 1 m minute pipeline"
    # params = config_parser.get_parameters_for_job("as_op", "b_t_s.ff_sam")
    # return get_databricks_job_runs_submit_json(entrypoint, description, params)


def get_processed_versions_logger_job_json():
    print_hello("_3", sleep_time=10)
    # entrypoint = "b_t_s_processed_verions_logger_as_op"
    # description = "B T S log processed versions pipeline"
    # params = config_parser.get_parameters_for_job("common", "processed_versions_logger")
    # return get_databricks_job_runs_submit_json(entrypoint, description, params)


@task(task_id=DELETE_CHECKPOINT_DIR_TASK_ID)
def delete_checkpoint_dir():
    print_hello("_4", sleep_time=5)
    # azure_login()
    #
    # storage_account_name = config_parser.get_configs().get("datalake.storage_account_name")
    # silver_container_name = config_parser.get_configs().get("datalake.silver_container_name")
    # checkpoint_dir = config_parser.get_configs().get("datalake.as_op.b_t_s.checkpoint_dir")
    #
    # if dir_exists(checkpoint_dir, storage_account_name, silver_container_name):
    #     delete_dir(checkpoint_dir, storage_account_name, silver_container_name)


def update_task_status(**kwargs):
    recoverable_tasks = [DELETE_CHECKPOINT_DIR_TASK_ID]
    failed_tasks = []
    task_instances = kwargs["dag_run"].get_task_instances()
    print(f"These are the tasks:")
    # for task_instance in task_instances:

    for task_instance in task_instances:
        print(f"Id of task is: {task_instance.task_id}")
        print(f"task_instance.current_state() != State.SUCCESS: {task_instance.current_state() != State.SUCCESS}")
        print(f"task_instance.task_id != kwargs[task_instance].task_id: {task_instance.task_id != kwargs['task_instance'].task_id}")
        if (
            task_instance.current_state() != State.SUCCESS
            and task_instance.task_id != kwargs["task_instance"].task_id
        ):
            print(f"inside first if condition and putting {task_instance.task_id} in failed tasks list")
            failed_tasks.append(task_instance.task_id)

    print(f"failed tasks: {failed_tasks} with first member; {failed_tasks[0]} with failed_tasks[0] in recoverable_tasks:{failed_tasks[0] in recoverable_tasks}")

    if failed_tasks:
        if failed_tasks[0] in recoverable_tasks:
            logger.warning(
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


@dag(
    dag_id=DAG_ID,
    default_args={
        "owner": "airflow",
        "email_on_failure": False,
        "depends_on_past": True,
        "wait_for_downstream": False,
        "retries": 2,
        "retry_delay": datetime.timedelta(seconds=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": datetime.timedelta(minutes=1),
    },
    start_date=datetime.datetime(2022, 12, 1),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["as_op", "silver", "sample"],
)
def b_t_s_workflow():
    (
        PythonOperator(
            task_id=DELTA_V_R_TASK_ID, python_callable=get_delta_rec_job_json
        )
        >> PythonOperator(
            task_id=CLEAN_AND_SAM_TASK_ID,
            python_callable=get_clean_and_sam_5_s_job_json,
        )
        >> PythonOperator(
            task_id=FF_AND_SAM_TASK_ID,
            python_callable=get_ff_and_sam_1_m_job_json,
            outlets=["as_op_dataset"],
        )
        >> PythonOperator(
            task_id=PROCESSED_VERSIONS_LOGGER_TASK_ID,
            python_callable=get_processed_versions_logger_job_json,
        )
        >> delete_checkpoint_dir()
        >> PythonOperator(
            task_id=UPDATE_STATUS_TASK_ID,
            provide_context=True,
            retries=0,
            python_callable=update_task_status,
            trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
        )
    )


b_t_s_workflow()
