import pandas as pd
import os

import math
import pandas as pd
import matplotlib as plt
import matplotlib.pyplot as plt
import io
from PIL import Image
import xlsxwriter

from io import BytesIO
import matplotlib.pyplot as plt
import re
import xlsxwriter
import io
from PIL import Image
from matplotlib.backends.backend_pdf import PdfPages


def sum_ins_table(temp_sum=None):
    start_col = 1
    end_col = 3
    ans = []
    for index in range(3):
        first_df = temp_sum.loc[:, start_col:end_col]
        first_df.columns = ['期货公司会员简称', '成交量', '比上交易日增减']
        # first_df['成交量'].astype(float)
        # first_df['比上交易日增减'].astype(float)
        # first_df[['成交量', '比上交易日增减']] = first_df[['成交量', '比上交易日增减']].astype(float, copy=True)
        # print(first_df['比上交易日增减'])

        first_df['成交量'] = first_df['成交量'].astype(float, copy=False)
        first_df['比上交易日增减'] = first_df['比上交易日增减'].astype(float, copy=False)
        # first_df.rename(columns=['期货公司会员简称', '成交量', '比上交易日增减'], inplace=True)
        first_ans = first_df.groupby(['期货公司会员简称', ])['成交量', '比上交易日增减'].sum() \
            .sort_values(by=['成交量'], ascending=False).reset_index().head(20)
        # print(first_ans)
        ans.append(first_ans)
        start_col += 4
        end_col += 4
    return ans


# 'CJ1':'总成交量',	'CJ1_CHG':'比上交易日增减',	'PARTICIPANTABBR1':'会员类别',
# 'CJ2':'总持买单量',	'CJ2_CHG':'比上交易日增减',	'':'会员类别',	'CJ3':'总持卖单量',	'CJ3_CHG':'比上交易日增减',
def split_by_productname(df=None):
    grouped = df.groupby(['PRODUCTNAME', ])
    # df.replace({ r'\A\s+|\s+\Z': '', '\n' : ' '}, regex=True, inplace=True)
    # df1['employee_id'] = df1['employee_id'].str.replace(" ","")
    for product in grouped.groups:
        product_df = grouped.get_group(product)
        print(product_df.head())

        # product_df = product_df.str.strip()
        product_df['PARTICIPANTABBR1'] = product_df['PARTICIPANTABBR1'].str.strip()
        ins_groups = product_df.groupby(['INSTRUMENTID'])
        clean_df = product_df[['PARTICIPANTABBR1', 'CJ1', 'CJ1_CHG', 'PRODUCTNAME']]
        # ~df1['nat_actn_2_3'].isin(['100','101','102','103','104'])
        clean_df = clean_df[~clean_df['PARTICIPANTABBR1'].isin(['期货公司','非期货公司','',]) ]
        test_df = clean_df.groupby(['PARTICIPANTABBR1', 'PRODUCTNAME'], as_index=False).first().reset_index()

        print(test_df.head())
        first_ans = clean_df.groupby(['PARTICIPANTABBR1', 'PRODUCTNAME' ])[
            'CJ1', 'CJ1_CHG'].sum() \
            .sort_values(by=['CJ1'], ascending=False).reset_index().head(20)
        print(first_ans)
        # test1_df = clean_df.groupby(['PARTICIPANTABBR1', ])[
        #     'CJ1', 'CJ1_CHG'].sum() \
        #     .sort_values(by=['CJ1'], ascending=False).unstack().reset_index().head(20)
        # print(test1_df)
        # first_ans =first_ans.reset_index(drop=True)
        # PRODUCTNAME
        # test = first_ans.first().reset_index()
        # print(test)
        print(first_ans)
        for ins in ins_groups.groups:
            ins_df = ins_groups.get_group(ins)
            print(ins_df)
    return

def split_ins(df):
    temp_index = df.index[df[0].str.contains('商品名称', case=False)].tolist()
    index_length = len(temp_index)
    ans_list = []
    for id in range(index_length):
        ins_df = None
        if id == index_length - 1:
            ins_df = df[temp_index[id]:]
        else:
            ins_df = df[temp_index[id]:temp_index[id + 1]]
        ins_df = ins_df.reset_index(drop=True)
        # print(ins_df)
        tb_index = ins_df.index[ins_df[0].str.contains('名次', case=False)].tolist()
        ins_header = ins_df[:(tb_index[0] - 1)]
        # 商品名称 ：铜                  2018-02-23
        ins_name = ins_header.iloc[0, 0]
        # import re
        x = re.split("：", ins_name)
        x = x[1].split()
        instrument = x[0]
        instrument_date = x[1]
        ins_name.split()
        temp_sum = None
        length = len(tb_index)
        for index, item in enumerate(tb_index):
            temp_df_1 = None
            if (index == length - 1):

                temp_df_1 = ins_df[(tb_index[index] + 1):-2]

            else:
                temp_df_1 = ins_df[(tb_index[index] + 1):(tb_index[index + 1] - 2)]
                # temp_df_1 = cu_df[(index + 1):(index+1 - 2)]
            if temp_sum is None:
                temp_sum = temp_df_1
            else:
                temp_sum = temp_sum.append(temp_df_1)
        print(temp_sum)
        print('-' * 100 + instrument + '-' * 100)
        rankings = sum_ins_table(temp_sum)
        print(rankings)
        print('-' * 100 + instrument + '-' * 100)
        ans = {}
        ans['instrument'] = instrument
        ans['date'] = instrument_date
        ans['ranking'] = rankings
        ans_list.append(ans)
    return ans_list

def sum_each_ins_to_pdf():
    a_file = './excel_files/排名表2018-02-23.csv'
    df = pd.read_csv(a_file, names=range(12), encoding='gbk')

    ans = split_ins(df)
    # test = ans[0]
    img_list = []
    filename = "期货公司排名"
    pdf = PdfPages('{filename}.pdf'.format(filename=filename))
    for test in ans:
        test_df = test['ranking'][0]
        instrument = test['instrument']
        date = test['date']
        print(test_df)


        # plt.xticks(rotation=90)
        plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
        plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

        fig, axes = plt.subplots(nrows=2, ncols=1, )
        fig.set_figheight(6)
        fig.set_figwidth(8)
        # test_df['time'] = test_df['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
        ax = test_df[['期货公司会员简称', '成交量']].plot(
            x='期货公司会员简称', linestyle='-', marker='o', ax=axes[0])
        ax2 = test_df[['期货公司会员简称', '比上交易日增减']].plot(
            x='期货公司会员简称', linestyle='-', marker='o', secondary_y=True, ax=axes[0])
        test_df[['期货公司会员简称', '成交量']].plot(x='期货公司会员简称', kind='bar'
                                          , sharex=True
                                          , ax=axes[1])
        axes[1].xaxis.set_tick_params(rotation=45)


        for t in axes[1].xaxis.get_ticklabels():
            temp = t.get_text()
            temp = temp.strip()
            # t.set_fontsize(13)
            if temp == '中信期货':
                print(t.get_text())
                # t.set_fontsize(13)
                t.set_color('red')
                t.set_weight('extra bold')

        fig.suptitle('期货品种：{instrument} 时间：{date}'.format(instrument=instrument, date=date)
                     , fontsize=14, fontweight='bold');
        #
        # import io
        # from PIL import Image
        # import xlsxwriter
        #
        # from io import BytesIO
        # import matplotlib.pyplot as plt
        imgdata = BytesIO()

        fig.savefig(imgdata, format="png")
        imgdata.seek(0)
        img_list.append(imgdata)
        pdf.savefig(fig)
        # plt.show()
    pdf.close()
    # import xlsxwriter
    workbook = xlsxwriter.Workbook('期货公司排名.xlsx')
    worksheet = workbook.add_worksheet("期货公司排名")
    num = 5
    for img in img_list:
        # import io
        # from PIL import Image

        #
        # import xlsxwriter
        #
        # workbook = xlsxwriter.Workbook('期货公司排名.xlsx')
        # worksheet = workbook.add_worksheet("期货公司排名")

        image_width = 140.0
        image_height = 182.0

        cell_width = 64.0
        cell_height = 20.0

        x_scale = cell_width / image_width * 10
        y_scale = cell_height / image_height * 10

        # use with xlsxwriter
        image_path = 'sales.png'
        bound_width_height = (240, 240)
        worksheet.insert_image('B{num}'.format(num = num), image_path, {'image_data': img, })
        num +=30
        # worksheet.insert_image('B40', image_path, {'image_data': imgdata, })
    workbook.close()

        # plt.show()
    return

def sum_whole_exchange():
    a_file = './excel_files/排名表2018-02-23.csv'
    df = pd.read_csv(a_file, names=range(12), encoding='gbk')

    ans = split_ins(df)
    frames = []
    for test in ans:
        each_df = test['ranking'][0]
        instrument = test['instrument']
        date = test['date']
        frames.append(each_df)

    result = pd.concat(frames)
    first_ans = result.groupby(['期货公司会员简称', ])['成交量', '比上交易日增减'].sum() \
        .sort_values(by=['成交量'], ascending=False).reset_index().head(20)
    # print(result)
    print(first_ans)
        # first_ans = first_df.groupby(['期货公司会员简称', ])['成交量', '比上交易日增减'].sum() \
        #     .sort_values(by=['成交量'], ascending=False).reset_index().head(20)
        # print(test_df)
    # plt.xticks(rotation=90)
    test_df = first_ans
    img_list = []
    filename = "上期所汇总排名"
    pdf = PdfPages('{filename}.pdf'.format(filename=filename))
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

    fig, axes = plt.subplots(nrows=2, ncols=1, )
    fig.set_figheight(6)
    fig.set_figwidth(8)
    # test_df['time'] = test_df['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
    ax = test_df[['期货公司会员简称', '成交量']].plot(
        x='期货公司会员简称', linestyle='-', marker='o', ax=axes[0])
    ax2 = test_df[['期货公司会员简称', '比上交易日增减']].plot(
        x='期货公司会员简称', linestyle='-', marker='o', secondary_y=True, ax=axes[0])
    test_df[['期货公司会员简称', '成交量']].plot(x='期货公司会员简称', kind='bar'
                                      , sharex=True
                                      , ax=axes[1])
    axes[1].xaxis.set_tick_params(rotation=45)

    for t in axes[1].xaxis.get_ticklabels():
        temp = t.get_text()
        temp = temp.strip()
        # t.set_fontsize(13)
        if temp == '中信期货':
            print(t.get_text())
            # t.set_fontsize(13)
            t.set_color('red')
            t.set_weight('extra bold')

    fig.suptitle('期货品种：{instrument} 时间：{date}'.format(instrument='上期所', date=date)
                 , fontsize=14, fontweight='bold');
    #
    # import io
    # from PIL import Image
    # import xlsxwriter
    #
    # from io import BytesIO
    # import matplotlib.pyplot as plt
    imgdata = BytesIO()

    fig.savefig(imgdata, format="png")
    imgdata.seek(0)

    pdf.savefig(fig)
    # pdf.savefig(fig)
    # plt.show()
    pdf.close()
    return
def main():
    a_file = './上期所0327.csv'
    df = pd.read_csv(a_file, encoding='gbk')
    split_by_productname(df)
    # sum_whole_exchange()
    # a_file = './excel_files/排名表2018-02-23.csv'
    # df = pd.read_csv(a_file, names=range(12), encoding='gbk')
    #
    # ans = split_ins(df)
    # test = ans[0]

    return


if __name__ == "__main__":
    main()