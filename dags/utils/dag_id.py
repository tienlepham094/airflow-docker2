import pandas as pd
import dotenv, os
dotenv.load_dotenv()
path_os  = os.getenv("file_path")

class DagID:    
    # file_path = f'{path_os}/csv/time_modifier.csv'
    file_path = '/opt/airflow/dags/csv/time_modifier.csv'
    def __init__(self):
        self._read_dag_change()
    def _read_dag_change(self):
        job_name = []
        df = pd.read_csv(self.file_path)
        for index, row in df.iterrows():
            if row['status'] == 1:
                job_name.append(row['job_name'])
        self.job_name = job_name
    
    
    