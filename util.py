import json
import os
import datetime


def stamp2date(stamp):
    return datetime.datetime.fromtimestamp(stamp).date().isoformat()


def date2stamp(date):
    date_obj = datetime.datetime.strptime("{} 00:00:00".format(date), "%Y-%m-%d %H:%M:%S")
    return date_obj.timestamp()


def get_global_config():
    """获取全局配置"""
    with open('config.json') as f:
        config = json.loads(f.read())
    config['base_dir'] = os.getcwd()
    return config


global_config = get_global_config()
actions = global_config.get('projection')

data_dir = os.path.join(global_config.get('base_dir'), global_config.get('data_dir'))
log_dir = os.path.join(global_config.get('base_dir'), global_config.get('log_dir'))
result_dir = os.path.join(global_config.get('base_dir'), global_config.get('result_dir'))

user_log = os.path.join(data_dir, global_config.get('user_log', 'user_log.csv'))
user_action = os.path.join(data_dir, global_config.get('user_action', 'user_action.csv'))

null = ''
