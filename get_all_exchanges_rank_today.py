import datetime
from time import sleep

from shfe_spider import shfe_rank
from spider_dalian import get_dalian_ranks
from spider_zhenzhou import czce_scrape, czce_scrape_v2
from zhongjin_spider_final import cffex_rank
from multiprocessing import Pool, TimeoutError
import time
import os
import concurrent.futures
import urllib.request
import tushare as ts

def main():
    pool = Pool(processes=4)
    # cffex_rank_by_contract()
    # return
    # today =  datetime.datetime.now() - datetime.timedelta(days=1)

    today = datetime.datetime(2018, 5, 28)
    today = datetime.datetime.now()
    endday = today - datetime.timedelta(days=4)
    is_holiday = ts.is_holiday(today)
    is_holiday = ts.is_holiday('2018-07-02')
    # endday = datetime.datetime(2018, 5, 28)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        for i in range(1):
            from shfe_spider import getLastWeekDay
            ts.is_holiday(today)
            weekday = today
            is_holiday = ts.is_holiday(weekday)

            if weekday <= endday:
                break
            if is_holiday:
                continue
            print(weekday)

            executor.submit(cffex_rank, weekday.year, weekday.month, weekday.day)
            executor.submit(czce_scrape_v2, weekday.year, weekday.month, weekday.day)
            executor.submit(get_dalian_ranks, weekday.year, weekday.month, weekday.day)
            executor.submit(shfe_rank, weekday.year, weekday.month, weekday.day)
            weekday = getLastWeekDay(weekday)

    print("mother process ended!!!")
    return

if __name__ == '__main__':
    main()