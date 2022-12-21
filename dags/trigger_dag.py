import pandas as pd
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

from airflow.plugins_manager import AirflowPlugin
def render_list_sql(li):
    return ", ".join(li)



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
    if len(job_name) > 1:
        ti.xcom_push('job_name', str(tuple(job_name)))      
    elif len(job_name) == 1:
        xcom_value = "(" +  f"'{job_name[0]}'" + ")"
        ti.xcom_push('job_name', xcom_value)
    else:
        xcom_value = "(" + "''" + ")"
        ti.xcom_push('job_name', xcom_value)
        
    logging.info(tuple(job_name))   

def render_(arg):
    return ", ".join(arg)
def _reset_dag_change():
    df = pd.read_csv(file_path)
    df.loc[df['status'] == 1, 'status'] = 0
    df.to_csv(file_path, mode='w+', index = False)
    logging.info(df.head())


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
        sql = '''
            UPDATE dag
            SET is_paused = 'f', next_dagrun = NOW()
            WHERE dag_id IN {{ti.xcom_pull(key = 'job_name', task_ids = 'read_change')}} 
            '''  
    )
    reset_status = PythonOperator(
        task_id = "reset_status",
        python_callable = _reset_dag_change
    )
    reading >> update_sql_dag >> reset_status
    
    