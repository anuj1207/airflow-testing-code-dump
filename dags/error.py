from airflow.models import DagModel


def on_failure_pause(dag_id):
    return lambda x: DagModel(dag_id=dag_id).set_is_paused(is_paused=True)
    # orm_dag = DagModel(dag_id=self.dag_id)
    # orm_dag.set_is_paused(is_paused=True)


class ErrorHandler:
    def __init__(self, dag_id: str) -> None:
        self.dag_id = dag_id
        pass
