# # from common.base import BaseDag
# #
# # dag1 = BaseDag(pipeline_id="_pipeline_id_1")
# # dag2 = BaseDag(pipeline_id="_pipeline_id_2")
# #
# # # print(f"path is:")
# # dag1.b_t_s_workflow()
# # dag2.b_t_s_workflow()
#
# from common._pipeline_1 import *
# from common._pipeline_2 import *

import logging
from airflow.decorators import dag

from common.common_dag import *

PIPELINE_ID_1 = "_pipeline_1"
PIPELINE_ID_2 = "_pipeline_2"
PIPELINE_ID_3 = "_pipeline_3"
PIPELINE_ID_4 = "_pipeline_4"
PIPELINE_ID_5 = "_pipeline_5"

logger = logging.getLogger(__name__)
path_to_conf_1 = f"/opt/airflow/dags/migration/{PIPELINE_ID_1}.conf"
path_to_conf_2 = f"/opt/airflow/dags/migration/{PIPELINE_ID_2}.conf"
path_to_conf_3 = f"/opt/airflow/dags/migration/{PIPELINE_ID_3}.conf"
path_to_conf_4 = f"/opt/airflow/dags/migration/{PIPELINE_ID_4}.conf"
path_to_conf_5 = f"/opt/airflow/dags/migration/{PIPELINE_ID_5}.conf"


def get_path_to_conf_1(**kwargs):
    print(f"This is path to conf: [{path_to_conf_1}]")
    return path_to_conf_1
def get_path_to_conf_2(**kwargs):
    print(f"This is path to conf: [{path_to_conf_2}]")
    return path_to_conf_2
def get_path_to_conf_3(**kwargs):
    print(f"This is path to conf: [{path_to_conf_3}]")
    return path_to_conf_3
def get_path_to_conf_4(**kwargs):
    print(f"This is path to conf: [{path_to_conf_4}]")
    return path_to_conf_4
def get_path_to_conf_5(**kwargs):
    print(f"This is path to conf: [{path_to_conf_5}]")
    return path_to_conf_5


@dag(**get_dag_args(PIPELINE_ID_1))
def b_t_s_workflow_1():
    get_tasks(get_path_to_conf_1)


@dag(**get_dag_args(PIPELINE_ID_2))
def b_t_s_workflow_2():
    get_tasks(get_path_to_conf_2)


@dag(**get_dag_args(PIPELINE_ID_3))
def b_t_s_workflow_3():
    get_tasks(get_path_to_conf_3)


@dag(**get_dag_args(PIPELINE_ID_4))
def b_t_s_workflow_4():
    get_tasks(get_path_to_conf_4)


@dag(**get_dag_args(PIPELINE_ID_5))
def b_t_s_workflow_5():
    get_tasks(get_path_to_conf_5)


b_t_s_workflow_1()
b_t_s_workflow_2()
b_t_s_workflow_3()
b_t_s_workflow_4()
b_t_s_workflow_5()
