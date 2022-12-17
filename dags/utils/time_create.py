import pandas as pd
import dotenv, os
import json
from glob import glob
from get_time import get_modifier_time
from func import get_job_name
dotenv.load_dotenv()


path_os  = os.getenv("file_path")
file_path = f'{path_os}/dags/config/*.json'
if __name__ == "__main__":
    data = []
    for path in glob(file_path):
        data.append([get_job_name(path),get_modifier_time(path), get_modifier_time(path), 0])
    
    df = pd.DataFrame(data, columns=['job_name','creation_time', 'last_modification', 'status'])
    # create first creation csv file
    df.to_csv(f'{path_os}/dags/csv/time_modifier.csv', index = False)