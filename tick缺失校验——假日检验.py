'''
将缺失数据的目录提取日期，判断是否为假日。
'''

import os
import re
import tushare as ts
def get_lost_date():
    reobj = re.compile(r'\\([0-9]{4})([0-9]{2})\\IF\\[0-9]{2}([0-9]{2})\\')
    datelist = []
    with open('./fillna.txt') as f:
        linetxt = f.readline()
        while linetxt:
            result = reobj.search(linetxt)
            if result:
                datevalue = result.group(1)+'-'+result.group(2)+'-'+result.group(3)
                datelist.append(datevalue)
                pass
            linetxt = f.readline()

    datelist = list(set(datelist))
    datelist.sort()
    with open('./datelist_na.txt','w') as f :
        for date_item in datelist:
            is_holiday = ts.is_holiday(date_item)
            f.write(date_item+','+str(is_holiday)+'\n')



if __name__ == '__main__':
    get_lost_date()
    os.system('pause')