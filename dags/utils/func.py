import logging
import json
from datetime import datetime, timedelta, date
def log_details(*args, **kwargs):
    logging.info(f"My execution date is {kwargs['ds']}")
    logging.info(f"My execution date is {kwargs['execution_date']}")
    
def get_default_args(*args, **kwargs):
    default_args = {}
    for key, value in kwargs.items():
        if key == "start_date":
            if value == "yesterday":
                default_args[key] = date.today() - timedelta(days=1)
            elif value == "tomorrow":
                default_args[key] = date.today() + timedelta(days=1)
            else:
                date_time = value.split(',')
                default_args[key] = datetime(int(date_time[0]), int(date_time[1]), int(date_time[2]))
        elif key == "retry_delay":
            default_args[key] = timedelta(minutes=int(value))
        else:
            default_args[key] = value
    return default_args
def get_job_name(path):
    f = open(path)
    definition = json.load(f)
    return definition["name"]
