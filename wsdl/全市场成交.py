import os
import comtypes.client
import zeep
import pandas as pd
# import matplotlib as plt
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import io
from PIL import Image
import xlsxwriter
import io
from PIL import Image
import xlsxwriter
from io import BytesIO
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

def excel_to_pdf(infile=None, outfile=None):
    cwd = os.getcwd()
    infile = os.path.join(os.path.abspath(cwd), infile)
    outfile = os.path.join(os.path.abspath(cwd), outfile)
    # infile = "C:/Users/niejn/PycharmProjects/scrapy_demo/期货统计排名_交易所.xlsx"
    # outfile = "C:/Users/niejn/PycharmProjects/scrapy_demo/test5.pdf"
    app = comtypes.client.CreateObject('Excel.Application')
    app.Visible = False
    doc = app.Workbooks.Open(infile, ReadOnly=False)
    doc.ExportAsFixedFormat(0, outfile, 1, 0)
    doc.Close()
    app.Quit()
    comtypes.CoUninitialize()
    return

def getData(par_dict={"KSRQ": "2018-02-26", "JSRQ": "2018-02-26", "YYB": "1", "TJFL": "0"},
            lookout_str='cxSqlKHCJTJ_GPXZ'):
    wsdl = 'http://10.21.2.75:8080/service/LBEBusiness?wsdl'
    client = zeep.Client(wsdl=wsdl)
    # ns0:loginResult(message: xsd:string, result: xsd:int, sessionId: xsd:string)
    sessionId = client.service.login("ZXQH_GPXZ", "GPXZ123321", "myapp", "plain", "")
    print(client.service.login("ZXQH_GPXZ", "GPXZ123321", "myapp", "plain", ""))
    factory = client.type_factory('ns0')
    string_type = client.get_type('xsd:string')
    lbParameter_type = client.get_type('ns0:lbParameter')
    queryOption_type = client.get_type('ns0:queryOption')
    '''		tmap.put("KSRQ", "2018-02-26");
    		tmap.put("JSRQ", "2018-02-26");
    		tmap.put("YYB", "1");
    		tmap.put("TJFL", "0");'''
    # par_dict = {"KSRQ": "2018-02-26", "JSRQ": "2018-02-26", "YYB": "1", "TJFL": "0"}
    params = []
    for key in par_dict:
        val = par_dict[key]
        temp_lbParameter = lbParameter_type(name=key, value=val)
        params.append(temp_lbParameter)

    valueOption_type = client.get_type('ns0:valueOption')
    valueOption = valueOption_type('VALUE')
    batchNo = 1
    batchSize = 3000
    ans_records = []
    mqueryOption = queryOption_type(batchNo=batchNo, batchSize=batchSize, queryCount=True, valueOption=valueOption)

    ans = client.service.query(sessionId.sessionId, lookout_str, params, "", mqueryOption)
    ans_records.extend(ans.records)

    while ((ans.result > 0) & ans.hasMore):
        batchNo += 1
        print('batchNo:{batchNo}'.format(batchNo=batchNo))

        mqueryOption = queryOption_type(batchNo=batchNo, batchSize=batchSize, queryCount=True, valueOption=valueOption)
        ans = client.service.query(sessionId.sessionId, lookout_str, params, "", mqueryOption)
        ans_records.extend(ans.records)
        # print(ans)
        print('batchNo:{batchNo}'.format(batchNo=batchNo))

    print(client.service.logout(sessionId.sessionId))
    return ans_records

def get_historydata(start_date='2018-06-01', end_date='2018-07-02'):
    par_dict = {"KSRQ": start_date, "JSRQ": end_date, "ZB": "0", "ZQ": "0", "TJFL": "0"}
    lookout_str = 'SqlGPXZGSSCQS'

    records = getData(par_dict, lookout_str)
    print(records)
    clean_records = []
    for a_record in records:
        clean_records.append(a_record.values)
    df = pd.DataFrame(clean_records, columns=['trade_type', 'time', 'sum', ])
    print(df.head())
    df['time'] = pd.to_datetime(df['time'])
    df['sum'] = df['sum'].astype(float)
    df['sum'] = df['sum'] / df['sum'].max()

    df_market = df[df['trade_type'] == '市场']
    df_corpor = df[df['trade_type'] != '市场']

    join_df = df_market.set_index('time').join(df_corpor.set_index('time'), lsuffix='_market', rsuffix='_corpor')
    print(join_df['sum_market'])

    return


def get_historydata_exchange(start_date='2018-06-01', end_date='2018-07-02'):
    par_dict = {"KSRQ": start_date, "JSRQ": end_date, "ZB": "0", "ZQ": "0", "TJFL": "0"}
    lookout_str = 'SqlGPXZGSSCQS'
    exchange_dict = {'1': 'DCE', '2': 'SHFE', '3': 'CFFEX', '4': 'CZCE', }
    df_dict = {}
    for key in exchange_dict:
        par_dict['JYS'] = key
        records = getData(par_dict, lookout_str)
        print(records)
        clean_records = []

        for a_record in records:
            clean_records.append(a_record.values)
        if not clean_records:
            return

        df = pd.DataFrame(clean_records, columns=['trade_type', 'time', 'sum', ])
        df['exchange'] = exchange_dict[key]
        print(df)
        print(df.head())
        df['time'] = pd.to_datetime(df['time'])
        df['time'] = df['time'].dt.strftime('%Y-%m-%d')

        df_market = df[df['trade_type'] == '市场']
        df_corpor = df[df['trade_type'] != '市场']
        print(df_corpor)

        df_dict[exchange_dict[key]] = df_corpor
    return df_dict

def plot_to_excel(df_dict=None):

    img_list = []
    for key in df_dict:
        test_df = df_dict[key].copy()
        test_df = test_df.reset_index()
        print(test_df.columns)
        plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
        plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
        fig, axes = plt.subplots(nrows=2, ncols=1, )
        fig.set_figheight(4)
        fig.set_figwidth(6)
        test_df['time'] = test_df['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
        ax = test_df[['time', 'sum_corpor']].plot(
            x='time', linestyle='-', marker='o', ax=axes[0])
        ax2 = test_df[['time', 'sum_market']].plot(
            x='time', linestyle='-', marker='o', secondary_y=True, ax=axes[0])
        test_df[['time', 'sum_corpor', 'sum_market']].plot(x='time', kind='bar'

                                                           , logy=True
                                                           , sharex=True
                                                           , ax=axes[1])

        def currency(x, pos):
            'The two args are the value and tick position'
            if x >= 1000000:
                return '${:1.1f}M'.format(x * 1e-6)
            return '${:1.0f}K'.format(x * 1e-3)

        def percent(x, pos):
            return '%.2f%%' % (x * 100)

        formatter = FuncFormatter(percent)
        axes[0].yaxis.set_major_formatter(formatter)
        ax2.yaxis.set_major_formatter(formatter)
        axes[1].yaxis.set_major_formatter(formatter)
        fig.suptitle('{exchange}期货交易所成交市场占比'.format(exchange=key), fontsize=14, fontweight='bold');
        imgdata = BytesIO()
        fig.savefig(imgdata, format="png")
        imgdata.seek(0)
        img_list.append(imgdata)
    workbook = xlsxwriter.Workbook('期货持仓分析报告.xlsx')
    worksheet = workbook.add_worksheet("全市场成交统计")
    num = 1
    for img in img_list:
        # imgdata.seek(0)
        # use with xlsxwriter
        image_path = 'sales.png'
        bound_width_height = (240, 240)
        worksheet.insert_image('A{num}'.format(num=num), image_path, {'image_data': img, })
        # image_path = '{exchange}期货交易所成交市场占比.png'.format(exchange=key)
        # worksheet.insert_image('A{pos}'.format(pos=str(img_pos)), image_path, {'image_data': imgdata, })
        num += 30

    workbook.close()

    filename = "期货持仓分析报告"
    excel_to_pdf('{filename}.xlsx'.format(filename=filename), '{filename}.pdf'.format(filename=filename))
    return
def pre_main():
    par_dict = {"KSRQ": "2018-01-01", "JSRQ": "2018-02-23", "ZB": "0", "ZQ": "0", "TJFL": "0"}
    lookout_str = 'SqlGPXZGSSCQS'

    records = getData(par_dict, lookout_str)
    print(records)
    clean_records = []
    for a_record in records:
        clean_records.append(a_record.values)
    df = pd.DataFrame(clean_records, columns=['trade_type', 'time', 'sum', ])
    print(df.head())
    df['time'] = pd.to_datetime(df['time'])
    df['sum'] = df['sum'].astype(float)
    df['sum'] = df['sum'] / df['sum'].max()


    df_market = df[df['trade_type'] == '市场']
    df_corpor = df[df['trade_type'] != '市场']

    join_df = df_market.set_index('time').join(df_corpor.set_index('time'), lsuffix='_market', rsuffix='_corpor')
    print(join_df['sum_market'])
    print()
    print(join_df)
    test_df = join_df.copy()
    test_df = test_df.reset_index()

    # plt.figure()
    # plt.xticks(rotation=90)
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

    fig, axes = plt.subplots(nrows=2, ncols=1, )
    fig.set_figheight(4)
    fig.set_figwidth(6)
    test_df['time'] = test_df['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
    ax = test_df[['time', 'sum_corpor']].plot(
        x='time', linestyle='-', marker='o', ax=axes[0])
    ax2 = test_df[['time', 'sum_market']].plot(
        x='time', linestyle='-', marker='o', secondary_y=True, ax=axes[0])
    test_df[['time', 'sum_corpor', 'sum_market']].plot(x='time', kind='bar'

                                                       , logy=True
                                                       , sharex=True
                                                       , ax=axes[1])

    def currency(x, pos):
        'The two args are the value and tick position'
        if x >= 1000000:
            return '${:1.1f}M'.format(x * 1e-6)
        return '${:1.0f}K'.format(x * 1e-3)

    def percent(x, pos):
        return '%.2f%%' % (x * 100)

    formatter = FuncFormatter(percent)
    axes[0].yaxis.set_major_formatter(formatter)

    ax2.yaxis.set_major_formatter(formatter)

    axes[1].yaxis.set_major_formatter(formatter)

    fig.suptitle('中信期货期货成交市场占比', fontsize=14, fontweight='bold');

    imgdata = BytesIO()

    fig.savefig(imgdata, format="png")
    imgdata.seek(0)

    workbook = xlsxwriter.Workbook('期货持仓分析报告.xlsx')
    worksheet = workbook.add_worksheet("全市场成交统计")

    image_width = 140.0
    image_height = 182.0
    cell_width = 64.0
    cell_height = 20.0
    x_scale = cell_width / image_width * 1
    y_scale = cell_height / image_height * 1

    # worksheet.insert_image('B2', 'python.png',
    #                        {'x_scale': x_scale, 'y_scale': y_scale})

    # use with xlsxwriter
    image_path = 'sales.png'
    bound_width_height = (240, 240)

    # worksheet.insert_image('A15', image_path, {'image_data': imgdata, 'x_scale': x_scale, 'y_scale': y_scale})
    worksheet.insert_image('A15', image_path, {'image_data': imgdata, })
    worksheet.insert_image('A40', image_path, {'image_data': imgdata, })
    workbook.close()
    plt.show()
    filename = "期货持仓分析报告"
    excel_to_pdf('{filename}.xlsx'.format(filename=filename), '{filename}.pdf'.format(filename=filename))
    return
def main():
    df_dict = get_historydata_exchange()
    # df_dict['CFFEX']
    print(df_dict['CFFEX'])
    # plot_to_excel(df_dict)
    return


if __name__ == '__main__':
    main()
