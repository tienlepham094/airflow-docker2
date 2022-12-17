import os.path, time
from glob import glob
from datetime import datetime

def convert_to_date(date_str: str):
    default_format = "%a %b %d %H:%M:%S %Y"
    return  datetime.strptime(date_str, default_format)
def get_modifier_time(path: str):
    return convert_to_date(time.ctime(os.path.getmtime(path)))

