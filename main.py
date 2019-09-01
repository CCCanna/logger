# 本来这一块要分开的，然而发现好像分不开。。。
import parser
import pandas
from util import *


def worker(user_id):
    query = log_data[log_data['openid'] == user_id]
    ip, date, stamp, action,_ = [list(i) for i in query.values.T]


if os.path.isfile(user_log):
    log_data = pandas.read_csv(user_log)
else:
    log_data = parser.main()

date_list = sorted(list(set(log_data.loc[:, 'date'])))
user_id_pool = set(log_data.loc[:, 'openid'])
action_data = list()

while user_id_pool:
    a_user = user_id_pool.pop()
    if a_user:
        action_data.append(worker(a_user))
