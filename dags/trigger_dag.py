import pandas as pd
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime



default_args = {
    'start_date': datetime(2022, 12, 17)
}
file_path = "/opt/airflow/dags/csv/time_modifier.csv"
job_name = []
def _read_dag_change():
    df = pd.read_csv(file_path)
    for index, row in df.iterrows():
        if row['status'] == 1:
            job_name.append(row['job_name'])
    logging.info(job_name)
def _reset_dag_change():
    df = pd.read_csv(file_path)
    df.loc[df['status'] == 1, 'status'] = 0
    df.to_csv(file_path, mode='w+', index = False)
    logging.info(df.head())

            

with DAG('trigger_dag', 
    schedule_interval='30 * * * *', 
    default_args=default_args, 
    catchup=False) as dag:

    reading = PythonOperator(
        task_id='read_change',
        python_callable=_read_dag_change
    )
    reset_status = PythonOperator(
        task_id = "reset_status",
        python_callable = _reset_dag_change
    )
    # trigger_target = TriggerDagRunOperator(
    #     task_id='trigger_target',
    #     trigger_dag_id='demo_dynamic_dag',
    #     execution_date='{{ ds }}',
    #     wait_for_completion=True,
    #     reset_dag_run=True
    # )
    reading  >> reset_status