import xml
from urllib.parse import urlencode
from urllib.request import urlopen, Request
import json
from urllib.request import urlopen
import json
import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
from lxml import html
import xml.etree.ElementTree as ET
import io
from urllib.parse import urlencode
from urllib.request import urlopen, Request
import json
from io import StringIO, BytesIO
from lxml import html
from lxml import etree
import requests
import re
import pandas as pd
# http://www.czce.com.cn/portal/DFSStaticFiles/Future/2018/20180522/FutureDataHolding.htm
'''
GET /portal/DFSStaticFiles/Future/2018/20180628/FutureDataHolding.htm HTTP/1.1
Host: www.czce.com.cn
Connection: keep-alive
Cache-Control: max-age=0
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
Referer: http://www.czce.com.cn/portal/jysj/qhjysj/ccpm/A09112003index_1.htm
Accept-Encoding: gzip, deflate
Accept-Language: zh-CN,zh;q=0.9
Cookie: td_cookie=1564901783; td_cookie=324009229; BIGipServerwww_cbd=842836160.23067.0000; JSESSIONID=yD4rb1Pf9mGVLJ5pLg2Bvh2f1l71v3KRTL2FL2Tc97R6Lbb2Rm3M!-1757810877; TS014ada8c=0169c5aa32b57ab6ef8c086dbd844958f7df06b421802765a6efee56515dff3cc1a0f7f27985ceccae56d4fce5a2ce8dd921fe2213
If-Modified-Since: Thu, 28 Jun 2018 07:49:11 GMT
'''
def czce_scrape_variety_only(year=2018, month=6, day=11):



    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Origin': 'http://www.czce.com.cn',
        'Upgrade-Insecure-Requests': '1',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Host': 'www.czce.com.cn',
        'Referer': 'http://www.czce.com.cn/portal/jysj/qhjysj/ccpm/A09112003index_1.htm',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36',

    }
    url = 'http://www.czce.com.cn/portal/DFSStaticFiles/Future/' \
          '{year:>04}/{year:>04}{month:>02}{day:>02}/FutureDataHolding.htm'.format(
                                                                                        year=year, month=month, day=day)
    base_url = "http://www.czce.com.cn/portal/jysj/qhjysj/ccpm/A09112003index_1.htm"
    session = requests.Session()
    session.headers.update(headers)
    base_html = session.get(base_url)
    print(base_html.text)
    print(session.headers)
    session.headers.update(headers)
    print(session.headers)

    s = session.get(url,)
    # print(s.text)
    s.encoding='gbk'
    print(s.text)
    from lxml import html
    tree = html.fromstring(s.text)
    table = tree.xpath('//table[@id="senfe"]')[0]
    data = [
        [td.text_content().strip() for td in row.findall('td')]
        for row in table.findall('tr')
    ]



    df = pd.DataFrame(data, )

    df[0] = df[0].str.replace('\xa0', '')
    print(df)
    form_header = ['品种', '合约', '合计']
    temp_index = []
    start_index = []
    import collections
    header_index_dict = collections.OrderedDict()
    for a_header in form_header:
        a_index_list = df.index[df[0].str.contains(a_header)].tolist()
        if not a_index_list:
            continue
        header_index_dict.update({a_header: a_index_list})

    if '品种' in header_index_dict:
        contracts = header_index_dict['品种']
        if '合计' not in header_index_dict:
            print("Error")
        end_index = header_index_dict['合计']
        for beg, end in zip(contracts, end_index):
            t_df = df[beg:end]
            t_df = t_df.applymap(lambda x: x.replace(',', '') if x else x)
            t_df.reset_index(inplace=True, drop=True)
            print(t_df)
            h_str = t_df.iat[0, 0]
            # '品种：苹果AP 日期：2018-05-23'
            import re
            # m = re.match(
            #     r"品种：(?P<productname>[\u4e00-\u9fa5]+)(?P<instrumentid>[a-zA-Z]+)\W*日期：(?P<date>[\d-]+)", h_str)
            m = re.match(
                r"品种：(?P<productname>[\u4e00-\u9fa5]+)?(?P<instrumentid>[a-zA-Z]+)\W*日期：(?P<date>[\d-]+)",
                h_str)
            t_instrumentid = m.group('instrumentid')
            t_productname = m.group('productname')
            if not t_productname:
                t_productname = t_instrumentid
            t_date = m.group('date')
            # productname 铜 instrumentid cu1804
            t_df = t_df.drop([0, 1])
            t_df['instrumentid'] = t_instrumentid
            t_df['productname'] = t_productname

            print(t_df)
            col_names = ['RANK', 'PARTICIPANTABBR1', 'CJ1', 'CJ1_CHG',
                         'PARTICIPANTABBR2', 'CJ2', 'CJ2_CHG',
                         'PARTICIPANTABBR3', 'CJ3', 'CJ3_CHG',
                         'INSTRUMENTID', 'PRODUCTNAME',]
            t_df.columns = col_names
            t_df['VARIETY'] = True;
            print(t_df)
            from db_insert2 import set_ranks_df

            set_ranks_df(t_df, year=year, month=month, day=day, exchange='CZCE')



    return

def czce_scrape_v2(year=2018, month=6, day=11):



    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Origin': 'http://www.czce.com.cn',
        'Upgrade-Insecure-Requests': '1',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Host': 'www.czce.com.cn',
        'Referer': 'http://www.czce.com.cn/portal/jysj/qhjysj/ccpm/A09112003index_1.htm',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36',

    }
    url = 'http://www.czce.com.cn/portal/DFSStaticFiles/Future/' \
          '{year:>04}/{year:>04}{month:>02}{day:>02}/FutureDataHolding.htm'.format(
                                                                                        year=year, month=month, day=day)
    base_url = "http://www.czce.com.cn/portal/jysj/qhjysj/ccpm/A09112003index_1.htm"
    session = requests.Session()
    session.headers.update(headers)
    base_html = session.get(base_url)
    print(base_html.text)
    print(session.headers)
    session.headers.update(headers)
    print(session.headers)

    s = session.get(url,)
    # print(s.text)
    s.encoding='gbk'
    print(s.text)
    from lxml import html
    tree = html.fromstring(s.text)
    table = tree.xpath('//table[@id="senfe"]')[0]
    data = [
        [td.text_content().strip() for td in row.findall('td')]
        for row in table.findall('tr')
    ]



    df = pd.DataFrame(data, )

    df[0] = df[0].str.replace('\xa0', '')
    print(df)
    form_header = ['品种', '合约', '合计']
    temp_index = []
    start_index = []
    import collections
    header_index_dict = collections.OrderedDict()
    for a_header in form_header:
        a_index_list = df.index[df[0].str.contains(a_header)].tolist()
        if not a_index_list:
            continue
        header_index_dict.update({a_header: a_index_list})

    if '品种' in header_index_dict:
        contracts = header_index_dict['品种']
        if '合计' not in header_index_dict:
            print("Error")
        end_index = header_index_dict['合计']
        for beg, end in zip(contracts, end_index):
            t_df = df[beg:end]
            t_df = t_df.applymap(lambda x: x.replace(',', '') if x else x)
            t_df.reset_index(inplace=True, drop=True)
            print(t_df)
            h_str = t_df.iat[0, 0]
            # '品种：苹果AP 日期：2018-05-23'
            import re
            # m = re.match(
            #     r"品种：(?P<productname>[\u4e00-\u9fa5]+)(?P<instrumentid>[a-zA-Z]+)\W*日期：(?P<date>[\d-]+)", h_str)
            m = re.match(
                r"品种：(?P<productname>[\u4e00-\u9fa5]+)?(?P<instrumentid>[a-zA-Z]+)\W*日期：(?P<date>[\d-]+)",
                h_str)
            t_instrumentid = m.group('instrumentid')
            t_productname = m.group('productname')
            if not t_productname:
                t_productname = t_instrumentid
            t_date = m.group('date')
            # productname 铜 instrumentid cu1804
            t_df = t_df.drop([0, 1])
            t_df['instrumentid'] = t_instrumentid
            t_df['productname'] = t_productname

            print(t_df)
            col_names = ['RANK', 'PARTICIPANTABBR1', 'CJ1', 'CJ1_CHG',
                         'PARTICIPANTABBR2', 'CJ2', 'CJ2_CHG',
                         'PARTICIPANTABBR3', 'CJ3', 'CJ3_CHG',
                         'INSTRUMENTID', 'PRODUCTNAME',]
            t_df.columns = col_names
            t_df['VARIETY'] = True;
            print(t_df)
            from db_insert2 import set_ranks_df

            set_ranks_df(t_df, year=year, month=month, day=day, exchange='CZCE')

    if '合约' in header_index_dict:
        instruments_index = header_index_dict['合约']
        len_instruments_index = len(instruments_index)
        if '合计' not in header_index_dict:
            print("Error")
        end_index = header_index_dict['合计']
        end_index = end_index[-len_instruments_index:]
        for beg, end in zip(instruments_index, end_index):
            t_df = df[beg:end]
            t_df = t_df.applymap(lambda x: x.replace(',', '') if x else x)
            t_df.reset_index(inplace=True, drop=True)
            print(t_df)
            h_str = t_df.iat[0, 0]
            import re
            # m = re.match(
            #     r"品种：(?P<productname>[\u4e00-\u9fa5]+)(?P<instrumentid>[a-zA-Z]+)\W*日期：(?P<date>[\d-]+)", h_str)
            m = re.match(
                r"合约：(?P<productname>[\u4e00-\u9fa5]+)?(?P<instrumentid>[a-zA-Z\d]+)\W*日期：(?P<date>[\d-]+)",
                h_str)
            t_instrumentid = m.group('instrumentid')
            t_productname = m.group('productname')
            if not t_productname:
                t_productname = t_instrumentid
            t_date = m.group('date')
            # productname 铜 instrumentid cu1804
            t_df = t_df.drop([0, 1])
            t_df['INSTRUMENTID'] = t_instrumentid
            t_df['PRODUCTNAME'] = t_productname

            print(t_df)
            col_names = ['RANK', 'PARTICIPANTABBR1', 'CJ1', 'CJ1_CHG',
                         'PARTICIPANTABBR2', 'CJ2', 'CJ2_CHG',
                         'PARTICIPANTABBR3', 'CJ3', 'CJ3_CHG',
                         'INSTRUMENTID', 'PRODUCTNAME', ]
            t_df.columns = col_names

            t_df['VARIETY'] = False;
            print(t_df)
            from db_insert2 import set_ranks_df

            set_ranks_df(t_df, year=year, month=month, day=day, exchange='CZCE')

    return

def czce_scrape(year=2018, month=6, day=11):
    from lxml import html
    url = 'http://www.czce.com.cn/portal/DFSStaticFiles/Future/' \
          '{year:>04}/{year:>04}{month:>02}{day:>02}/FutureDataHolding.htm'.format(year=year, month=month, day=day)

    parser = html.HTMLParser(encoding='gbk')
    # root = html.document_fromstring(content, parser=parser)
    try:
        tree = html.parse(url, parser=parser)
    except Exception as e:
        print(e)
        return
    # tree.docinfo
    table = tree.findall('//table')[1]
    data = [
        [td.text_content().strip() for td in row.findall('td')]
        for row in table.findall('tr')
    ]

    df = pd.DataFrame(data, )

    df[0] = df[0].str.replace('\xa0', '')
    print(df)
    form_header = ['品种', '合约', '合计']
    temp_index = []
    start_index = []
    import collections
    header_index_dict = collections.OrderedDict()
    for a_header in form_header:
        a_index_list = df.index[df[0].str.contains(a_header)].tolist()
        if not a_index_list:
            continue
        header_index_dict.update({a_header: a_index_list})

    if '品种' in header_index_dict:
        contracts = header_index_dict['品种']
        if '合计' not in header_index_dict:
            print("Error")
        end_index = header_index_dict['合计']
        for beg, end in zip(contracts, end_index):
            t_df = df[beg:end]
            t_df = t_df.applymap(lambda x: x.replace(',', '') if x else x)
            t_df.reset_index(inplace=True, drop=True)
            print(t_df)
            h_str = t_df.iat[0, 0]
            # '品种：苹果AP 日期：2018-05-23'
            import re
            # m = re.match(
            #     r"品种：(?P<productname>[\u4e00-\u9fa5]+)(?P<instrumentid>[a-zA-Z]+)\W*日期：(?P<date>[\d-]+)", h_str)
            m = re.match(
                r"品种：(?P<productname>[\u4e00-\u9fa5]+)?(?P<instrumentid>[a-zA-Z]+)\W*日期：(?P<date>[\d-]+)",
                h_str)
            t_instrumentid = m.group('instrumentid')
            t_productname = m.group('productname')
            if not t_productname:
                t_productname = t_instrumentid
            t_date = m.group('date')
            # productname 铜 instrumentid cu1804
            t_df = t_df.drop([0, 1])
            t_df['instrumentid'] = t_instrumentid
            t_df['productname'] = t_productname
            t_df['date'] = t_date
            print(t_df)
            col_names = ['RANK', 'PARTICIPANTABBR1', 'CJ1', 'CJ1_CHG',
                         'PARTICIPANTABBR2', 'CJ2', 'CJ2_CHG',
                         'PARTICIPANTABBR3', 'CJ3', 'CJ3_CHG',
                         'INSTRUMENTID', 'PRODUCTNAME', 'DATE']
            t_df.columns = col_names
            t_df['VARIETY'] = True;
            print(t_df)
            from db_insert2 import set_ranks_df

            set_ranks_df(t_df, year=year, month=month, day=day, exchange='CZCE')

    if '合约' in header_index_dict:
        instruments_index = header_index_dict['合约']
        len_instruments_index = len(instruments_index)
        if '合计' not in header_index_dict:
            print("Error")
        end_index = header_index_dict['合计']
        end_index = end_index[-len_instruments_index:]
        for beg, end in zip(instruments_index, end_index):
            t_df = df[beg:end]
            t_df = t_df.applymap(lambda x: x.replace(',', '') if x else x)
            t_df.reset_index(inplace=True, drop=True)
            print(t_df)
            h_str = t_df.iat[0, 0]
            import re
            # m = re.match(
            #     r"品种：(?P<productname>[\u4e00-\u9fa5]+)(?P<instrumentid>[a-zA-Z]+)\W*日期：(?P<date>[\d-]+)", h_str)
            m = re.match(
                r"合约：(?P<productname>[\u4e00-\u9fa5]+)?(?P<instrumentid>[a-zA-Z\d]+)\W*日期：(?P<date>[\d-]+)",
                h_str)
            t_instrumentid = m.group('instrumentid')
            t_productname = m.group('productname')
            if not t_productname:
                t_productname = t_instrumentid
            t_date = m.group('date')
            # productname 铜 instrumentid cu1804
            t_df = t_df.drop([0, 1])
            t_df['INSTRUMENTID'] = t_instrumentid
            t_df['PRODUCTNAME'] = t_productname

            print(t_df)
            col_names = ['RANK', 'PARTICIPANTABBR1', 'CJ1', 'CJ1_CHG',
                         'PARTICIPANTABBR2', 'CJ2', 'CJ2_CHG',
                         'PARTICIPANTABBR3', 'CJ3', 'CJ3_CHG',
                         'INSTRUMENTID', 'PRODUCTNAME', ]
            t_df.columns = col_names

            t_df['VARIETY'] = False;
            print(t_df)
            from db_insert2 import set_ranks_df

            set_ranks_df(t_df, year=year, month=month, day=day, exchange='CZCE')

    return

def main():
    import datetime
    today = datetime.datetime(2018, 6, 16)
    endday = datetime.datetime(2018, 5, 31)
    for i in range(30):
        from shfe_spider import getLastWeekDay
        weekday = getLastWeekDay(today)
        today = weekday
        if today <= endday:
            break
        print(weekday)
        try:
            czce_scrape_variety_only(year=weekday.year, month=weekday.month, day=weekday.day)
        except Exception as e:
            print(e)
        from time import sleep
        sleep(2)
    # czce_scrape()
    # html_parse()
    # test2()
    return
if __name__ == '__main__':
    main()