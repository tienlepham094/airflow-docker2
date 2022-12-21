import pandas as pd
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from utils.dag_id import DagID

default_args = {
    'start_date': datetime(2022, 12, 17)
}
file_path = "/opt/airflow/dags/csv/time_modifier.csv"
def _reset_dag_change():
    df = pd.read_csv(file_path)
    df.loc[df['status'] == 1, 'status'] = 0
    df.to_csv(file_path, mode='w+', index = False)
    logging.info(df.head())
    

dag_id = DagID()


with DAG('trigger_dag_2', 
    schedule_interval='@daily', 
    default_args=default_args, 
    catchup=False) as dag:

    reset_status = PythonOperator(
        task_id = "reset_status",
        python_callable = _reset_dag_change
    )
    trigger_dag = []
    for i in dag_id.job_name:
        trigger_target = TriggerDagRunOperator(
            task_id=f'trigger_{i}',      
            trigger_dag_id= i,
            execution_date='{{ ds }}',
            wait_for_completion=True, 
            reset_dag_run=True
        )
        trigger_dag.append(trigger_target)  
    trigger_dag >> reset_status