import pandas as pd
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime


default_args = {
    'start_date': datetime(2022, 12, 17)
}
file_path = "/opt/airflow/dags/csv/time_modifier.csv"

def _read_dag_change(ti):
    job_name = []
    df = pd.read_csv(file_path)
    for index, row in df.iterrows():
        if row['status'] == 1:
            job_name.append(row['job_name']) 
    tuple_job = tuple(job_name)    
    ti.xcom_push('job_name', tuple_job)
    # ti.xcom_push('hello', 'hello')
    logging.info(tuple_job)
def _reset_dag_change():
    df = pd.read_csv(file_path)
    df.loc[df['status'] == 1, 'status'] = 0
    df.to_csv(file_path, mode='w+', index = False)
    logging.info(df.head())

sql = '''
UPDATE dag
SET is_paused = 'f'
WHERE dag_id IN {{(ti.xcom_pull(key = 'job_name', task_ids = 'read_change'))}};
'''            
# sql = '''
# SELECT dag_id FROM dag;
# '''   
with DAG('trigger_dag', 
    schedule_interval='0 * * * *', 
    default_args=default_args, 
    catchup=False,
    render_template_as_native_obj=True) as dag:

    reading = PythonOperator(
        task_id='read_change',
        python_callable=_read_dag_change
    )
    update_sql_dag = PostgresOperator(
        task_id = 'update_dag_table',
        postgres_conn_id = 'postgres',
        sql = sql
        # params = {'value': "{{ti.xcom_pull(key = 'job_name', task_ids = 'read_change')}}"}
    )
    reset_status = PythonOperator(
        task_id = "reset_status",
        python_callable = _reset_dag_change
    )
    reading >> update_sql_dag >> reset_status
    
    