import pandas as pd
import dotenv, os
from glob import glob
from get_time import get_modifier_time
from func import get_job_name
dotenv.load_dotenv()



def get_new_modifier(file_path):
    data = []
    for path in glob(file_path):
        data.append([get_modifier_time(path), get_job_name(path)])
    return data

def read_csv(file_path):
    return pd.read_csv(file_path)

def compare_time(df, data, path_os):
    if df.shape[0] < len(data):
        col_list = list(df['job_name'])
        for i in range(len(data)):
            if data[i][1] not in col_list:
                new_row = {
                "job_name": get_job_name(data[i][1]) ,
                "time_creation": data[i][0],
                "last_modification": data[i][0],
                "status": 1
            }
                df = pd.concat([pd.DataFrame([new_row]), df], ignore_index=True)

    for index, row in df.iterrows():
        for i in range(len(data)):
            if data[i][1] == row['job_name']:
                if str(data[i][0]) != row ['last_modification']:
                    df.loc[index, ['status']] = 1
                    df.loc[index, ['last_modification']] = data[i][0]

    print(df.head())
        
    # write new csv
    df.to_csv(f'{path_os}/dags/csv/time_modifier.csv', mode='w+', index = False) 
        
        
if __name__ == "__main__":
    path_os  = os.getenv("file_path")
    file_path = f'{path_os}/dags/config/*.json'
    data_new = get_new_modifier(file_path)
    df = read_csv(f'{path_os}/dags/csv/time_modifier.csv')
    compare_time(df, data_new, path_os)

    
        