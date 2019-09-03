import re
import time
import urllib.parse

import pandas

from util import *


def get_location(location):
    """Map location to word that shows what user had done."""
    if "/admin/" in location:
        return "管理员"
    return actions.get(location, null)


def get_date(string):
    """仅仅返回日期和时间戳"""
    string = string[1:-1]
    time_array = time.strptime(string)
    return time.mktime(time_array)


def split_query(query):
    """仅仅返回url中的openid"""
    query = query.replace("zq_", "")
    slices = query.split("&")
    try:
        query_dict = dict([piece.split("=") for piece in slices])
    except ValueError:
        return null
    return query_dict.get("openid", null)


def parse_url(string):
    """仅仅返回openid和路径"""
    parsed = urllib.parse.urlparse(string)
    return split_query(parsed.query), parsed.path


def match(regex, string):
    """Please ensure the number of tokens should keep up with variables that receive result from this method."""
    pattern = re.compile(regex)
    if pattern.findall(string):
        result, *_ = pattern.findall(string)
    else:
        return [null]
    return result


def shrink_time(timestamp):
    """把时间戳的后三位消掉，消除相对冗余的数据，但是这样用户使用时长没办法统计了555～。
    毕竟没办法了又在赶ddl，人家也不想暴力处理啊"""
    return str(int(timestamp))[:-3]


def parse(string):
    """Use regular expression to catch required data and return a list."""
    string = re.sub(r"[ ]{2,}", " ", string)
    if len(string) < 128:
        return null
    ip = match(r"\d+\.\d+\.\d+\.\d+", string)
    time_series = match(r"\[\w{3} \w{3}.*201\d\]", string)
    stamp = get_date(time_series)
    _, url = match(r"(POST|GET|HEAD) (.*) =>", string)
    openid, location = parse_url(url)
    results = [ip, stamp2date(stamp), shrink_time(stamp), get_location(location), openid]
    return results


def main():
    print("step 1 running... it will take about 2 minutes.")
    error = open(os.path.join(data_dir, 'unmatched.txt'), "w+")
    values = list()
    # 处理起来发现ip这个字段貌似对数据去重会有干扰。。。
    ip_set = set()

    for filename in os.listdir(log_dir):
        handle = open(os.path.join(log_dir, filename))
        buffer = handle.readlines()
        for line in buffer:
            result = parse(line)
            if result:
                ip_set.add(result.pop(0))
                values.append(result)
            else:
                error.write("{}\n".format(line))
        handle.close()
    error.close()

    data_header = ['date', 'stamp', 'action', 'openid']
    frame = pandas.DataFrame(values, columns=data_header).drop_duplicates()
    frame.to_csv(os.path.join(data_dir, user_log), index=None)
    ip_frame = pandas.DataFrame(ip_set, columns=['ip'])
    ip_frame.to_csv(user_ip, index=None)
    print('parsing from journal files finished.')
    return ip_frame, frame


if __name__ == '__main__':
    main()
