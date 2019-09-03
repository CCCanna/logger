# 直接对用户数据进行分析，由于先前的破坏处理，无法统计使用时长，部分数据未使用如掌武数据库中的openid对照表
import parser
import pandas
from util import *


def get_days():
    """获取所有的天的datetime对象"""
    pool = pandas.Series(pandas.to_datetime((log_data[:]['date'])))
    summary_data.update({"number": pool.count(), 'start': str(pool.min().date()), 'end': str(pool.max().date())})
    return pandas.array(pool.drop_duplicates())


def get_day_strings():
    """获取所有的日期"""
    result = sorted([str(day.date()) for day in get_days()])
    return result


def weeks_first_day():
    """生成字典，每周的最后一天作为键，每周的日期作为值"""
    series = get_days()
    result = list()
    for date in sorted(series):
        if not date.weekday():
            result.append(str(date.date()))
    last_day = str(max(get_days()).date())
    if last_day not in result:
        result.append(last_day)
    return result


def summary_by_date():
    """按日期统计的用户操作数量"""
    result = log_data.groupby('date').size().to_dict()
    return result, count_by_week(result)


def count_by_week(data):
    """统计每个星期的操作数量和用户数量"""
    first_days = weeks_first_day()
    result = {}
    for index, day in enumerate(first_days):
        count = 0
        for key in sorted(data.keys()):
            if index > 0 and day >= key > first_days[index - 1]:
                count += data.get(key)
            elif index == 0 and key <= day:
                count += data.get(key)
        result[day] = count
    return result


def get_daily_users():
    """计算每天的用户数量，返回每日用户数，每周用户数"""
    strings = get_day_strings()
    results = {}
    for date in strings:
        result = log_data[log_data['date'] == date].groupby('openid').size().shape[0]
        results[date] = result
    return results, count_by_week(results)


def get_user_id():
    identifiers = set(log_data.loc[:, 'openid'])
    summary_data.update({'users': len(identifiers)})
    return identifiers


def summary_by_user_id():
    """按用户统计的每日平均操作数量，可以从此处进行用户M/W/DAU的处理，请等待后续完善"""
    identifiers = get_user_id()
    data = list()
    for identifier in identifiers:
        result = log_data[log_data['openid'] == identifier].groupby('date').size().to_dict()
        data.append(map_by_date(result))
    frame = pandas.DataFrame(data, columns=get_day_strings())
    return frame.mean().to_dict()


def map_by_date(data):
    """保证生成的DataFrame形状正常"""
    strings = get_day_strings()
    result = [0] * len(strings)
    for index, string in enumerate(strings):
        result[index] = data.get(string, pandas.np.nan)
    return result


def assume_user_type(date_map):
    """推断用户类型及活跃"""
    pass


def summary_by_feature():
    """按功能统计的用户数量，即每种功能有多少人使用，返回功能的使用人数，功能的使用次数"""
    legends = set(actions.values())
    result_users = {}
    result_count = {}
    for leg in legends:
        query = log_data[log_data['action'] == leg].groupby('openid').size()
        result_users[leg] = query.shape[0]
        result_count[leg] = query.sum()
    return result_users, result_count


def commit_data(title, data_map, column_name, chart_type='line', is_title=False):
    """保存数据"""
    result2csv(title, column_name, data_map, is_title)
    result2sqlite(title, data_map, chart_type, is_title)


def result2csv(title, column_name, data_map, is_title):
    """数据转成csv"""
    if not is_title:
        frame = map2frame(data_map, column_name)
    else:
        frame = pandas.DataFrame([title], columns=['title'])
    frame.to_csv(ensure_file(result_dir, title), index=None)


def map2frame(data_map, column_name):
    """数据转成DataFrame"""
    buffer = []
    for key in sorted(data_map.keys()):
        buffer.append((key, data_map.get(key)))
    return pandas.DataFrame(buffer, columns=column_name)


def result2sqlite(title, data_map, chart_type='line', is_title=False):
    """数据存入result.db便于Highcharts化"""
    if is_title:
        frame = pandas.DataFrame([data_map])
        frame.to_sql('title', engine, if_exists='replace', index=None)
        return
    legends = ['title', 'chart_type', 'legs', 'data']
    map2sql(data_map, [title, chart_type], legends)


def map2sql(projection, already, let):
    """数据存入result.db便于Highcharts化"""
    buffer = []
    for key in sorted(projection.keys()):
        buffer.append([key, projection.get(key)])
    buff_frame = pandas.DataFrame(buffer)
    legs, legs_data = buff_frame.T.values
    already.extend([','.join(legs), ','.join([str(x) for x in legs_data])])
    frame = pandas.DataFrame([already], columns=let)
    frame.to_sql('charts', engine, if_exists='append', index=None)


# 检测文件是否存在，不存在则重新计算
if os.path.isfile(user_log):
    log_data = pandas.read_csv(user_log)
else:
    _, log_data = parser.main()

summary_data = {'operations': log_data.shape[0]}
daily_operation, weekly_operation = summary_by_date()
data_legend = ['日期/周', '活跃数']
commit_data('每日活跃数', daily_operation, data_legend, chart_type='bar')
commit_data('每周活跃数', weekly_operation, data_legend, chart_type='pie')

daily_user, weekly_user = get_daily_users()
commit_data('每日用户数', daily_user, data_legend, chart_type='bar')
commit_data('每周用户数', weekly_user, data_legend, chart_type='pie')

feature_user, feature_count = summary_by_feature()
data_legend = ['功能', '数量']
commit_data('功能使用人数', feature_user, data_legend, chart_type='bar')
commit_data('功能使用次数', feature_count, data_legend, chart_type='pie')

get_user_id()

# 下面操作时间过长暂时注释
# average_use = summary_by_user_id()
# data_legend = ['日期/周', '活跃数']
# commit_data('平均每日活跃', average_use, data_legend, chart_type='line')

# 统计总数
summary_data['avg'] = summary_data.get('operations') / summary_data.get('users')
summary = """在{start}到{end}这{number}天里，共有{users}人使用了{operations}次掌上武大的诸多功能，人均活跃{avg}次""".format(**summary_data)
commit_data('summary', {'title': summary}, ['title'], is_title=True)
