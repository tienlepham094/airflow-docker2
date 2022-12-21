# from datetime import datetime, timedelta, date
# import json
# def get_default_args(dict,*args, **kwargs):
#     default_args = {}
#     for key, value in dict.items():
#         if key == "start_date":
#             if value == "yesterday":
#                 default_args[key] = date.today() - timedelta(days=1)
#             elif value == "tomorrow":
#                 default_args[key] = date.today() + timedelta(days=1)
#             else:
#                 date_time = value.split(',')
#                 default_args[key] = datetime(int(date_time[0]), int(date_time[1]), int(date_time[2]))
#         elif key == "retry_delay":
#             default_args[key] = timedelta(minutes=int(value))
#         else:
#             try:
#                 default_args[key] = int(value)
#             except:
#                 default_args[key] = value
#     return default_args
# file_path = './dags/config/config.json'
# with open(file_path) as f:
#     data = json.load(f)
#     print(get_default_args(data['default_args']))

    