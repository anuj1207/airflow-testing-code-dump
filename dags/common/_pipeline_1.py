import datetime
import logging
from airflow.decorators import dag

from common.common_dag import *

# from utils.error_handler import on_failure_pause
# from utils.azure import azure_login, delete_dir, dir_exists
# from utils.config_parser import ConfigParser
# from utils.databricks_job_json_generator import DatabricksJobJsonGenerator

PIPELINE_ID = "_pipeline_1"

logger = logging.getLogger(__name__)
path_to_conf = f"/opt/airflow/dags/migration/{PIPELINE_ID}.conf"


# config_parser = ConfigParser(
# )


def get_path_to_conf(**kwargs):
    print(f"This is path to conf: [{path_to_conf}]")
    return path_to_conf


@dag(
    dag_id=BASE_DAG_ID + PIPELINE_ID,
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
def b_t_s_workflow():
    get_tasks(get_path_to_conf)

b_t_s_workflow()
