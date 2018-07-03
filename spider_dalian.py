from time import sleep
from urllib.parse import urlencode
from urllib.request import urlopen, Request
import json
from io import StringIO, BytesIO
from lxml import html
from lxml import etree
import requests
import re
import pandas as pd


def trim_df():
    t_df = pd.read_csv('t_df_dalian.csv')
    a_index_list = t_df.index[t_df['rank'].str.contains('总计')].tolist()
    print(t_df)
    t_df.drop(t_df[t_df['rank'].str.contains('总计')].index, inplace=True, axis='index')
    t_df.drop(['rank2', 'rank3'], axis='columns', inplace=True)
    print(t_df)
    return

# month 为真实月份减少1
def get_dce_ranks(year=2018, month=6, day=14, variety_list=None,):
    minus_month = int(month) - 1
    values = {
        'memberDealPosiQuotes.variety': 'b',
        'memberDealPosiQuotes.trade_type': '0',
        'year': str(year),
        'month': str(minus_month),
        'day': str(day),
        'contract.contract_id': 'all',
        'contract.variety_id': 'b',
    }

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Origin':'http://www.dce.com.cn',
        'Upgrade-Insecure-Requests':'1',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Host': 'www.dce.com.cn',
        'Referer': 'http://www.dce.com.cn/publicweb/quotesdata/memberDealPosiQuotes.html',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36',

    }
    url = "http://www.dce.com.cn/publicweb/quotesdata/memberDealPosiQuotes.html"
    base_url = "http://www.dce.com.cn/publicweb/quotesdata/memberDealPosiQuotes.html"
    session = requests.Session()
    session.headers.update(headers)
    base_html = session.get(base_url)
    print(base_html.text)
    print(session.headers)
    session.headers.update(headers)
    print(session.headers)





    df = None
    '''
    memberDealPosiQuotes.variety: b
memberDealPosiQuotes.trade_type: 0
year: 2018
month: 5
day: 14
contract.contract_id: b1901
contract.variety_id: b
contract: 
'''
    for a_var in variety_list:
        for instrument in variety_list[a_var]:
            values['contract.variety_id'] = a_var
            values['memberDealPosiQuotes.variety'] = a_var
            values['contract.contract_id'] = instrument
            s = session.post(url, values,)
            print(s.text)
            # //*[@id="memberDealPosiQuotesForm"]/div/div[1]/div[4]/div/ul[2]/li[1]
            tree = html.fromstring(s.text)

            tables = tree.xpath('//*[@id="printData"]/div/table[2]')
            if not tables:
                continue
            table = tables[0]
            print(table)
            data = [
                [td.text_content().strip() for td in row.findall('td')]
                for row in table.findall('tr')
            ]
            print(data)
            if len(data) <= 1:
                continue
            col_names = ['RANK', 'PARTICIPANTABBR1', 'CJ1', 'CJ1_CHG',
                         'RANK2', 'PARTICIPANTABBR2', 'CJ2', 'CJ2_CHG',
                         'RANK3', 'PARTICIPANTABBR3', 'CJ3', 'CJ3_CHG',
                         ]
            t_df = pd.DataFrame(data, columns=col_names)
            # t_df = t_df.applymap(lambda x: x.replace(',', ''))
            t_df.dropna(axis='index', inplace=True)
            t_df = t_df.applymap(lambda x: x.replace(',', ''))
            # t_df.columns = col_names
            print(t_df)
            # year=2018, month=5, day=11
            t_df.to_csv('./csv/dce/t_df_dce_ins_{instrument}_{year}_{month}_{day}.csv'
                        .format(instrument=instrument, year=year, month=month, day=day), index=False)
            # df[0].str.contains(a_header)
            t_df.drop(t_df[t_df['RANK'].str.contains('总计')].index, inplace=True, axis='index')
            t_df.drop(['RANK2', 'RANK3'], axis='columns', inplace=True)
            print(t_df)
            t_df['INSTRUMENTID'] = instrument
            t_df['PRODUCTNAME'] = a_var
            import datetime
            today = datetime.datetime.now()
            # t_df['DATE'] = today
            if df is None:
                df = t_df
            else:
                df = df.append(t_df)

    print(df)
    df['VARIETY'] = False;
    df.to_csv('./csv/dce/t_df_dce_ins_sum_{year}_{month}_{day}.csv'.format(year=year, month=month, day=day), index=False)
    from db_insert2 import set_ranks_df
    set_ranks_df(df, year=year, month=month, day=day, exchange='DCE')
    return

# month 为真实月份减少1
def get_dalian_ranks(year=2018, month=6, day=14):
    minus_month = int(month) - 1
    values = {
        'memberDealPosiQuotes.variety': 'b',
        'memberDealPosiQuotes.trade_type': '0',
        'year': str(year),
        'month': str(minus_month),
        'day': str(day),
        'contract.contract_id': 'all',
        'contract.variety_id': 'b',
    }

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Origin':'http://www.dce.com.cn',
        'Upgrade-Insecure-Requests':'1',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Host': 'www.dce.com.cn',
        'Referer': 'http://www.dce.com.cn/publicweb/quotesdata/memberDealPosiQuotes.html',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36',

    }
    url = "http://www.dce.com.cn/publicweb/quotesdata/memberDealPosiQuotes.html"
    base_url = "http://www.dce.com.cn/publicweb/quotesdata/memberDealPosiQuotes.html"
    session = requests.Session()
    session.headers.update(headers)
    base_html = session.get(base_url)
    print(base_html.text)
    print(session.headers)
    session.headers.update(headers)
    print(session.headers)
    variety_list = ['a', 'b', 'm', 'y', 'p', 'c', 'cs', 'jd', 'fb', 'bb', 'l', 'v', 'pp', 'j', 'jm', 'i']
    cn_variety_names = ['豆一', '豆二', '豆粕', '豆油', '棕榈油', '玉米', '玉米淀粉', '鸡蛋', '纤维板',
                        '胶合板', '聚乙烯', '聚氯乙烯', '聚丙烯', '焦炭', '焦煤', '铁矿石',]


    df = None
    all_instrument_dict = {}
    for a_var, cn_name in zip(variety_list, cn_variety_names):
        values['contract.variety_id'] = a_var
        values['memberDealPosiQuotes.variety'] = a_var
        s = session.post(url, values,)
        sleep(2)
        print(s.text)
        # //*[@id="memberDealPosiQuotesForm"]/div/div[1]/div[4]/div/ul[2]/li[1]
        tree = html.fromstring(s.text)
        # keyWord_65
        instruments = tree.xpath('//ul[@class="keyWord clearfix"]/li[@class="keyWord_65"]')
        if instruments:
            instrument_ids = [
                row.text_content().strip()
                for row in instruments
            ]
            all_instrument_dict[a_var] = instrument_ids
            print(all_instrument_dict)
        temp_tables = tree.xpath('//*[@id="printData"]/div/table[2]')
        if not temp_tables:
            continue
        table = temp_tables[0]
        print(table)
        data = [
            [td.text_content().strip() for td in row.findall('td')]
            for row in table.findall('tr')
        ]
        print(data)
        if len(data) <= 1:
            continue
        col_names = ['RANK', 'PARTICIPANTABBR1', 'CJ1', 'CJ1_CHG',
                     'RANK2', 'PARTICIPANTABBR2', 'CJ2', 'CJ2_CHG',
                     'RANK3', 'PARTICIPANTABBR3', 'CJ3', 'CJ3_CHG',
                     ]
        t_df = pd.DataFrame(data, columns=col_names)
        # t_df = t_df.applymap(lambda x: x.replace(',', ''))
        t_df.dropna(axis='index', inplace=True)
        t_df = t_df.applymap(lambda x: x.replace(',', ''))
        # t_df.columns = col_names
        print(t_df)
        # year=2018, month=5, day=11
        t_df.to_csv('./csv/dce/t_df_dalian_{instrument}_{year}_{month}_{day}.csv'
                    .format(instrument=a_var, year=year, month=month, day=day), index=False)
        # df[0].str.contains(a_header)
        t_df.drop(t_df[t_df['RANK'].str.contains('总计')].index, inplace=True, axis='index')
        t_df.drop(['RANK2', 'RANK3'], axis='columns', inplace=True)
        print(t_df)
        t_df['INSTRUMENTID'] = a_var
        t_df['PRODUCTNAME'] = cn_name
        import datetime
        today = datetime.datetime.now()
        # t_df['DATE'] = today
        if df is None:
            df = t_df
        else:
            df = df.append(t_df)

    print(df)
    df['VARIETY'] = True;
    df.to_csv('./csv/dce/t_df_dalian_sum_{year}_{month}_{day}.csv'.format(year=year, month=month, day=day), index=False)
    from db_insert2 import set_ranks_df
    set_ranks_df(df, year=year, month=month, day=day, exchange='DCE')

    get_dce_ranks(year, month, day, all_instrument_dict)

    return


def main():
    # trim_df()
    import datetime
    today = datetime.datetime(2018, 6, 7)
    endday = datetime.datetime(2018, 5, 31)
    today = datetime.datetime.now()
    endday = today - datetime.timedelta(days=2)
    for i in range(30):
        from shfe_spider import getLastWeekDay
        weekday = getLastWeekDay(today)
        today = weekday
        if today <= endday:
            break
        print(weekday)
        get_dalian_ranks(year=weekday.year, month=weekday.month, day=weekday.day)
        from time import sleep
        sleep(2)

    return

if __name__ == '__main__':
    main()