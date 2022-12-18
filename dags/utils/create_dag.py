from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from utils.func import log_details


def create_dag(schedule, default_args, definition, spark_application= None, jars = None, catchup = False, *args, **kwargs):
    """Create dags dynamically."""
    with DAG(
        definition["name"], schedule_interval=schedule, default_args=default_args, catchup=catchup
    ) as dag:

        tasks = {}
        for node in definition["nodes"]:
            params = node["parameters"]
            operator = node["_type"]
            node_name = node["name"].replace(" ", "")
            params["task_id"] = node_name
            params["dag"] = dag
            if operator == "DummyOperator":
                tasks[node_name] = DummyOperator(**params)
            elif operator == "BashOperator":
                tasks[node_name] = BashOperator(**params)
            elif operator == "PythonOperator":
                func_params = globals()[node['func']]
                tasks[node_name] = PythonOperator(**params, python_callable = func_params)
            elif operator == "SparkSubmitOperator":
                if "application" in node:
                    params["application"] = f'{spark_application}/{node["application"]}'
                if "jars" in node:
                    params["jars"] = f'{jars}/{node["jars"]}'
                tasks[node_name] = SparkSubmitOperator(**params)
        for node_name, downstream_conn in definition["connections"].items():
            for ds_task in downstream_conn:
                tasks[node_name] >> tasks[ds_task]

    globals()[definition["name"]] = dag
    return dag