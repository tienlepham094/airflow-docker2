import json
from glob import glob
from airflow.models import Variable
from utils.create_dag import create_dag
from utils.func import get_default_args


spark_application = Variable.get("spark_folder")
file_path = "/opt/airflow/dags/config/*.json"

for file in glob(file_path):
    f = open(file)
    definition = json.load(f)
    default_args = get_default_args(**definition["default_args"])
    create_dag("@daily", default_args, definition, spark_application)




