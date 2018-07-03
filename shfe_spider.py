# http://www.shfe.com.cn/data/dailydata/kx/kx20180319.dat
import datetime
from time import sleep

import requests

from urllib.parse import urlencode
from urllib.request import urlopen, Request
import json
import pandas as pd
from bs4 import BeautifulSoup

from db_insert2 import set_type
# from shfe_ranking_com import split_by_productname


def getLastWeekDay(day=None):
    now=day

    if now.isoweekday()==1:
      dayStep=3
    else:
      dayStep=1

    lastWorkDay = now - datetime.timedelta(days=dayStep)
    return lastWorkDay





def crawl_openinterest(year, month, day):
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',

        'Host': 'www.shfe.com.cn',
        'Referer': 'http://www.shfe.com.cn/statements/dataview.html?paramid=pm',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36',

    }
    params = {'firstname': 'Ryan', 'lastname': 'Mitchell'}
    # url = 'http://www.shfe.com.cn/data/instrument/ContractBaseInfo20180321.dat'
    url = "http://www.shfe.com.cn/data/dailydata/kx/kx{year:>04}{mon:>02}{day:>02}.dat"\
        .format(year=year, mon=month, day=day)
    # url = "http://www.shfe.com.cn/data/dailydata/kx/kx20180319.dat"
    # http://www.shfe.com.cn/data/instrument/ContractDailyTradeArgument20180321.dat
    request = requests.post(url, headers=headers)
    print(request.request.headers)
    # print(request.headers)
    # response = urlopen(request, timeout=4)
    # re_data = response.read()
    # re_data = re_data.decode('utf8')
    print(request.text)
    comments = request.json()
    data = comments['o_curinstrument']
    issue = json.loads(request.text)
    df = pd.DataFrame(data)
    # openinterest_dict = get_openinterest(df)
    # # print(openinterest_dict)
    # return openinterest_dict

'''import requests
session = requests.Session()
params = {'username': 'username', 'password': 'password'}
s = session.post("http://pythonscraping.com/pages/cookies/welcome.print("Cookie is set to:")
print(s.cookies.get_dict())
print("-----------")
print("Going to profile page...")
s = session.get("http://pythonscraping.com/pages/cookies/profile.php")
print(s.text)'''
def shfe_rank(year=2018, month=3, day=27):
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',

        'Host': 'www.shfe.com.cn',
        'Referer': 'http://www.shfe.com.cn/statements/dataview.html?paramid=pm',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36',

    }

    # url = "http://www.shfe.com.cn/data/dailydata/kx/pm20180327.dat"
    url = "http://www.shfe.com.cn/data/dailydata/kx/pm{year:>04}{month:>02}{day:>02}.dat" \
        .format(year=year, month=month, day=day)

    request = requests.post(url, headers=headers)
    if request.status_code == requests.codes.ok:
        pass
    else:
        print('404 Request Page Not Found!')
        print(request.status_code)
        return
    # if requests.status_code == 404:
    #     print('404 Request Page Not Found!')
    # content = request.text
    # soup = BeautifulSoup(content, 'lxml')
    # divs = soup.find_all("div",align='center')
    # print(divs)
    # print(divs[0].get_text())
    # print(request.request.headers)

    print(request.text)
    comments = request.json()
    if 'o_curinstrument' in comments:
        data = comments['o_curinstrument']
    else:
        data = comments['o_cursor']
    issue = json.loads(request.text)
    df = pd.DataFrame(data)

    file_name = "./csv/shfe/shfe_{year:>04}{month:>02}{day:>02}.csv".format(year=year, month=month, day=day)
    df.to_csv(file_name, encoding='gbk', index=False)
    # insert to db
    df['VARIETY'] = False;
    set_type(df, year=year, month=month, day=day)


    return

def main():
    # today = datetime.date.today()
    today = datetime.datetime(2018, 6, 27)
    endday = datetime.datetime(2018, 5, 31)
    for i in range(30):
        weekday = getLastWeekDay(today)
        today = weekday
        if today <= endday:
            break
        print(weekday)
        shfe_rank(year=weekday.year, month=weekday.month, day=weekday.day)
        sleep(2)


    # headers = {
    #     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    #     'Accept-Encoding': 'gzip, deflate',
    #     'Accept-Language': 'zh-CN,zh;q=0.9',
    #     'Connection': 'keep-alive',
    #
    #     'Host': 'www.shfe.com.cn',
    #     'Referer': 'http://www.shfe.com.cn/statements/dataview.html?paramid=pm',
    #     'Upgrade-Insecure-Requests': '1',
    #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36',
    #
    # }
    # # params = {'firstname': 'Ryan', 'lastname': 'Mitchell'}
    # # url = 'http://www.shfe.com.cn/data/instrument/ContractBaseInfo20180321.dat'
    # # url = "http://www.shfe.com.cn/data/dailydata/kx/kx20180319.dat"
    # url = "http://www.shfe.com.cn/data/dailydata/kx/pm20180327.dat"
    # # http://www.shfe.com.cn/data/dailydata/kx/pm20180327.dat
    # # http://www.shfe.com.cn/data/instrument/ContractDailyTradeArgument20180321.dat
    # request = requests.post(url, headers=headers)
    # print(request.request.headers)
    # # print(request.headers)
    # # response = urlopen(request, timeout=4)
    # # re_data = response.read()
    # # re_data = re_data.decode('utf8')
    # print(request.text)
    # comments = request.json()
    # if 'o_curinstrument' in comments:
    #     data = comments['o_curinstrument']
    # else:
    #     data = comments['o_cursor']
    # issue = json.loads(request.text)
    # df = pd.DataFrame(data)
    # {'名次':'RANK',	'期货公司会员简称':'PARTICIPANTABBR1',	'成交量':'',
    #  '比上交易日增减':'CJ1_CHG',	'名次':'',	'期货公司会员简称':'PARTICIPANTABBR2',
    #  '持买单量':'',	'比上交易日增减':'CJ2_CHG',	'名次':'',
    #  '期货公司会员简称':'PARTICIPANTABBR3',	'持卖单量':'CJ3',
    #  '比上交易日增减/变化':'CJ3_CHG',
    #  }
    # df.to_csv('上期所0327.csv', encoding='gbk', index=False)
    # set_type(df)
    # split_by_productname(df)
    # 'DELIVERYMONTH' (2906757719856)
    # url = 'http://www.shfe.com.cn/data/dailydata/kx/kx20180319.dat'
    #
    # # r = requests.head(url=url, auth=auth)
    # r = requests.head(url=url, )
    # print(r.headers)
    # o_curinstrument

    return

if __name__ == '__main__':
    main()