import datetime
import logging
from airflow.decorators import dag

from common.common_dag import *

# from utils.error_handler import on_failure_pause
# from utils.azure import azure_login, delete_dir, dir_exists
# from utils.config_parser import ConfigParser
# from utils.databricks_job_json_generator import DatabricksJobJsonGenerator

PIPELINE_ID = "_pipeline_3"

logger = logging.getLogger(__name__)
path_to_conf = f"/opt/airflow/dags/migration/{PIPELINE_ID}.conf"


# config_parser = ConfigParser(
# )


def get_path_to_conf(**kwargs):
    print(f"This is path to conf: [{path_to_conf}]")
    return path_to_conf


@dag(**get_dag_args(PIPELINE_ID))
def b_t_s_workflow():
    get_tasks(get_path_to_conf)

b_t_s_workflow()
