import json
import os
import datetime
from sqlalchemy import create_engine


def get_sqlite_engine():
    """绝对路径四个'/'相对路径三个'/'由于是Linux所以是这样子的"""
    return create_engine("sqlite:///{}".format(os.path.join(result_dir, 'result.db')))


def stamp2date(stamp):
    return datetime.datetime.fromtimestamp(stamp).date().isoformat()


def date2stamp(date):
    date_obj = datetime.datetime.strptime("{} 00:00:00".format(date), "%Y-%m-%d %H:%M:%S")
    return date_obj.timestamp()


def get_global_config():
    """获取全局配置"""
    with open('config.json') as f:
        global_config = json.loads(f.read())
    global_config['base_dir'] = os.getcwd()
    return global_config


def ensure_path(root, path_slice):
    path = os.path.join(root, path_slice)
    if not os.path.exists(path):
        os.mkdir(path)
    return path


def ensure_file(base, name):
    path = os.path.join(base, name)
    if not path.endswith('.csv'):
        return path + '.csv'
    return path


# 下面是一些其它两个脚本都会用到的变量
config = get_global_config()
actions = config.get('projection')

data_dir = ensure_path(config.get('base_dir'), config.get('data_dir'))
log_dir = ensure_path(config.get('base_dir'), config.get('log_dir'))
result_dir = ensure_path(config.get('base_dir'), config.get('result_dir'))

user_log = ensure_file(data_dir, config.get('user_log', 'user_log.csv'))
user_action = ensure_file(data_dir, config.get('user_action', 'user_action.csv'))
user_ip = ensure_file(data_dir, config.get("user_ip", "user_ip.csv"))

engine = get_sqlite_engine()
null = ''
