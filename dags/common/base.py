import datetime
import logging
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

PIPELINE_ID = "_pipeline_1"
DAG_ID = "as_op_migration_workflow" + PIPELINE_ID
PIVOT_OLD_DATA_TASK_ID = "pivot_old_data"
DELTA_V_R_TASK_ID = "delta_v_r"
CLEAN_AND_SAM_TASK_ID = "clean_and_sam_5_s"
FF_AND_SAM_TASK_ID = "ff_and_sam_1_m"
PROCESSED_VERSIONS_LOGGER_TASK_ID = "processed_versions_logger"
DELETE_CHECKPOINT_DIR_TASK_ID = "delete_checkpoint_dir"
UPDATE_STATUS_TASK_ID = "update_task_status"


class BaseDag():
    logger = logging.getLogger(__name__)

    def __int__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        self.path_to_conf = f"/opt/airflow/dags/migration/{self.pipeline_id}.conf"

    def get_path_to_conf(self, **kwargs):
        print(f"This is path to conf: [{self.path_to_conf}]")
        return self.path_to_conf

    @dag(
        dag_id=DAG_ID,
        default_args={
            "owner": "airflow",
            "email_on_failure": False,
            "depends_on_past": True,
            "wait_for_downstream": False,
            "retries": 5,
            "retry_delay": datetime.timedelta(minutes=5),
            "retry_exponential_backoff": True,
            "max_retry_delay": datetime.timedelta(minutes=30),
        },
        start_date=datetime.datetime(2022, 12, 1),
        schedule="*/1 * * * *",
        catchup=False,
        max_active_runs=1,
        tags=["as_op", "silver", "hadoopmigration", "sample"],
    )
    def b_t_s_workflow(self):
        (
            PythonOperator(task_id=PIVOT_OLD_DATA_TASK_ID, python_callable=self.get_path_to_conf)
            # >> DatabricksSubmitRunDeferrableOperator(
            #     task_id=DELTA_V_R_TASK_ID, json=get_delta_rec_job_json()
            # )
            # >> DatabricksSubmitRunDeferrableOperator(
            #     task_id=CLEAN_AND_SAM_TASK_ID,
            #     json=get_clean_and_sam_5_s_job_json(),
            # )
            # >> DatabricksSubmitRunDeferrableOperator(
            #     task_id=FF_AND_SAM_TASK_ID,
            #     json=get_ff_and_sam_1_m_job_json(),
            # )
            # >> DatabricksSubmitRunDeferrableOperator(
            #     task_id=PROCESSED_VERSIONS_LOGGER_TASK_ID,
            #     json=get_processed_versions_logger_job_json(),
            # )
            # >> delete_checkpoint_dir()
            # >> PythonOperator(
            #     task_id=UPDATE_STATUS_TASK_ID,
            #     provide_context=True,
            #     retries=0,
            #     python_callable=update_task_status,
            #     trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
            # )
        )


# config_parser = ConfigParser(
# )

# def get_spark_delta_conf() -> Dict[str, str]:
#     conf = {
#         "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
#         "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact": "true",
#         "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true",
#         "spark.databricks.delta.schema.autoMerge.enabled": "true",
#         "spark.databricks.delta.formatCheck.enabled": "false",
#         "spark.databricks.delta.merge.repartitionBeforeWrite.enabled": "true",
#         "spark.databricks.adaptive.autoOptimizeShuffle.enabled": "true",
#         "spark.databricks.delta.autoCompact.minNumFiles": "6",
#         "spark.cleaner.referenceTracking.cleanCheckpoints": "true",
#     }
#     return conf


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


# def get_pivot_job_json():
#     entrypoint = "pivot_old_data_app"
#     description = "Migration: Pivot old As OP data"
#     params = config_parser.get_parameters_for_job("as_op_migration", "pivoting")
#     return get_databricks_job_runs_submit_json(entrypoint, description, params)
#
#
# def get_delta_rec_job_json():
#     entrypoint = "bronze_delta_v_r"
#     description = "Migration: B T S Delta V Rec"
#     params = config_parser.get_parameters_for_job("common", "delta_rec")
#     return get_databricks_job_runs_submit_json(entrypoint, description, params)
#
#
# def get_clean_and_sam_5_s_job_json():
#     entrypoint = "b_t_s_unfilled_as_op"
#     description = "Migration: B T S Clean and Sam 5 s pipeline"
#     params = config_parser.get_parameters_for_job("as_op_migration", "b_t_s.clean_sample")
#     return get_databricks_job_runs_submit_json(entrypoint, description, params)
#
#
# def get_ff_and_sam_1_m_job_json():
#     entrypoint = "b_t_s_forward_fill_as_op"
#     description = "Migration: B T S FF and Sam 1 m minute pipeline"
#     params = config_parser.get_parameters_for_job(
#         "as_op_migration", "b_t_s.ff_sam"
#     )
#     return get_databricks_job_runs_submit_json(entrypoint, description, params)
#
#
# def get_processed_versions_logger_job_json():
#     entrypoint = "b_t_s_processed_verions_logger_as_op"
#     description = "Migration: B T S log processed versions pipeline"
#     params = config_parser.get_parameters_for_job("common", "processed_versions_logger")
#     return get_databricks_job_runs_submit_json(entrypoint, description, params)
#
#
# @task(task_id=DELETE_CHECKPOINT_DIR_TASK_ID)
# def delete_checkpoint_dir():
#     azure_login()
#
#     storage_account_name = config_parser.get_configs().get("datalake.storage_account_name")
#     silver_container_name = config_parser.get_configs().get("datalake.silver_container_name")
#     checkpoint_dir = config_parser.get_configs().get(
#         "datalake.as_op_migration.b_t_s.checkpoint_dir"
#     )
#
#     if dir_exists(checkpoint_dir, storage_account_name, silver_container_name):
#         delete_dir(checkpoint_dir, storage_account_name, silver_container_name)


# def update_task_status(**kwargs):
#     failed_tasks = []
#     task_instances = kwargs["dag_run"].get_task_instances()
#     logger.info(f"Found tasks in the current DAG: {list(map(lambda x: x.task_id, task_instances))}")
#
#     for task_instance in task_instances:
#         is_task_not_success = task_instance.current_state() != State.SUCCESS
#         is_not_current_task = task_instance.task_id != kwargs["task_instance"].task_id
#         if is_task_not_success and is_not_current_task:
#             logger.info(
#                 f"Task {task_instance.task_id} is considered as failed due to [is_task_not_success: {is_task_not_success} and is_not_current_task: {is_not_current_task}]"
#             )
#             failed_tasks.append(task_instance.task_id)
#         else:
#             logger.info(f"Task {task_instance.task_id} is successful")
#
#     if failed_tasks:
#         logger.info(f"Failed tasks found: {failed_tasks}")
#         for task_instance in task_instances:
#             task_instance.set_state(state=State.FAILED)
#         raise Exception(
#             "Task {} failed. Marking all tasks failed and failing this DAG run".format(failed_tasks)
#         )
#     else:
#         logger.info("No task failed.")

# b_t_s_workflow()
