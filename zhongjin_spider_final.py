import datetime
import xml
from time import sleep

import requests

from urllib.parse import urlencode
from urllib.request import urlopen, Request
import json
import pandas as pd
from bs4 import BeautifulSoup
from lxml import html
import xml.etree.ElementTree as ET
import io
# from lxml import etree as ET
from db_insert2 import set_ranks_df


def cffex_rank(year=2018, month=3, day=27):
    contracts = ['IF', 'IC', 'IH', 'TF', 'T']
    # contracts = ['IC', 'IH', 'TF', 'T']
    for a_contract in contracts:
        cffex_rank_by_contract(year,month,day,a_contract)
        sleep(2)
    return


def cffex_rank_by_contract(year=2018, month=5, day=1, contract='IF'):
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        # 'Connection': 'keep-alive',

        # 'Host': 'www.shfe.com.cn',
        'Referer': 'http://www.cffex.com.cn/ccpm/',
        'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36',

    }  # params = {'firstname': 'Ryan', 'lastname': 'Mitchell'}
    # url = 'http://www.shfe.com.cn/data/instrument/ContractBaseInfo20180321.dat'
    # url = "http://www.shfe.com.cn/data/dailydata/kx/kx20180319.dat"
    url = "http://www.cffex.com.cn/sj/ccpm/201804/09/IF.xml"
    url = "http://www.cffex.com.cn/sj/ccpm/{year:>04}{month:>02}/{day:>02}/{contract}.xml"\
        .format(year=year, month=month, day=day, contract=contract)
    # http://www.shfe.com.cn/data/dailydata/kx/pm20180327.dat
    # http://www.shfe.com.cn/data/instrument/ContractDailyTradeArgument20180321.dat
    request = requests.get(url, headers=headers)
    if "gray_404" in request.text:
        return
    print(request.encoding)
    # request.encoding = 'gbk'
    print(request.text)
    print(request.encoding)
    # 网页错误 <td class="gray_404">您要查看的网页可能已被删除、名称已被更改，或者暂时不可用。</td>

    # response.encoding = 'gbk'
    # print
    # response.text
    try:
        root = ET.fromstring(request.text)
    except xml.etree.ElementTree.ParseError as e:
        print(e)
        return
    for child in root:
        print("_" * 30)
        print(child.tag, child.attrib)
        for grandchild in child:
            print(grandchild.tag, grandchild.attrib)

    def iter_docs(root):
        author_attr = root.attrib
        for doc in root.iter('data'):
            doc_dict = author_attr.copy()
            doc_dict.update(doc.attrib)
            doc_dict['data'] = doc.text
            yield doc_dict

    def iter_rows(root):

        for data in root.findall('data'):
            doc_dict = {}
            for attr in data:
                # doc_dict = {}
                val = data.find(attr.tag).text
                doc_dict[attr.tag] = val
                # print(attr.tag, val)
            print(doc_dict)
            yield doc_dict
            # pass

    t_li = list(iter_rows(root))
    print(t_li)
    doc_df = pd.DataFrame(t_li)
    # doc_df = doc_df.applymap(lambda x: x.replace(',', '') if x else x)
    print(doc_df)
    doc_df.to_csv("zhongjin_xml1.csv", encoding="gbk")
    df = doc_df.copy()
    df['rank'] = df['rank'].astype(int, copy=False)
    grouped = df.groupby(['instrumentid'])
    whole_df = None
    for gr in grouped.groups:
        print(gr)
        a_group_df = grouped.get_group(gr)
        grand_groups = a_group_df.groupby(['datatypeid'])
        # df.sort_values(by=['col1'])
        # print(a_group_df.sort_values(by=['rank']))
        print("-" * 60)
        temp_merge_df = None
        for grand_group in ['0', '1', '2']:

            print(grand_group)

            grand_group_df = grand_groups.get_group(grand_group)
            # print(grand_group_df.sort_values(by=['rank']))
            grand_group_df = grand_group_df.sort_values(by=['rank'])
            if temp_merge_df is None:

                temp_merge_df = grand_group_df

            else:
                selected_cols = ['rank', 'shortname', 'volume', 'varvolume', 'partyid']
                t_y_df = grand_group_df[selected_cols]

                name_mapper = {col: col + '_' + grand_group if col != 'rank' else col for col in selected_cols}


                t_y_df.rename(columns=lambda x: x.strip(), inplace=True)
                t_y_df.rename(columns=name_mapper, inplace=True)
                result = pd.merge(temp_merge_df, t_y_df, on='rank')
                temp_merge_df = result

        print(temp_merge_df)
        # temp_merge_df.to_csv("pandas_merge_0412.csv", encoding='gbk')
        if whole_df is None:
            whole_df = temp_merge_df
        else:

            result = pd.concat([whole_df, temp_merge_df], axis=0)
            whole_df = result
            # print(whole_df)
            whole_df.to_csv("whole_pandas_merge_0412_v2.csv", encoding='gbk')
            # break
            # break
    # whole_df['tradingday'] = pd.to_datetime(whole_df['tradingday'], format='%Y%m%d', errors='ignore')
    # print(whole_df['tradingday'])
    from db_insert2 import insert_db

    try:
        # Index(['rank', 'shortname_0', 'volume_0', 'varvolume_0', 'partyid_0',
      #  'shortname_1', 'volume_1', 'varvolume_1', 'partyid_1', 'shortname_2',
      #  'volume_2', 'varvolume_2', 'partyid_2'],
      # dtype='object')
        whole_df = whole_df[['instrumentid', 'productid', 'rank', 'shortname', 'tradingday', 'varvolume', 'volume',
                             'shortname_1', 'volume_1', 'varvolume_1', 'shortname_2', 'volume_2', 'varvolume_2', 'partyid',
                             'partyid_1', 'partyid_2']]
    except Exception as e:
        print(e)
    col_mapper = {'shortname_2': 'PARTICIPANTABBR3', 'shortname': 'PARTICIPANTABBR1', 'volume': 'CJ1',
                  'varvolume_2': 'CJ3_CHG', 'instrumentid': 'INSTRUMENTID', 'volume_2': 'CJ3',
                  'volume_1': 'CJ2', 'varvolume': 'CJ1_CHG', 'rank': 'RANK',
                  'shortname_1': 'PARTICIPANTABBR2', 'productid': 'PRODUCTNAME', 'varvolume_1': 'CJ2_CHG',
                  'tradingday': 'REPORT_DATE', 'partyid': 'PARTICIPANTID1', 'partyid_1': 'PARTICIPANTID2',
                  'partyid_2': 'PARTICIPANTID3'}


    whole_df.rename(columns=col_mapper, inplace=True)
    # whole_df['EXCHANGE'] = "CFFEX"
    whole_df['VARIETY'] = False;
    # t_y_df.rename(columns=name_mapper, inplace=True)
    print(whole_df)
    set_ranks_df(whole_df, year=year, month=month, day=day, exchange='CFFEX')
    # insert_db(whole_df, tablename='ranks', con='sqlite:///exchange.sqlite')
    return

def main():
    # cffex_rank_by_contract()
    # return
    today = datetime.datetime(2018, 6, 27)
    endday = datetime.datetime(2018, 5, 31)
    for i in range(30):
        from shfe_spider import getLastWeekDay
        weekday = getLastWeekDay(today)
        today = weekday
        if today <= endday:
            break
        print(weekday)
        cffex_rank(year=weekday.year, month=weekday.month, day=weekday.day)
        sleep(2)
    return

if __name__ == '__main__':
    main()