import json
from datetime import datetime, timedelta
from airflow.models import Variable
from utils.create_dag import create_dag
# Opening JSON file
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 12, 13),
    'retry_delay': timedelta(minutes=5)
}
file_path = Variable.get("file_path")
f = open(file_path)
definition = json.load(f)



create_dag("@daily", default_args, definition)




