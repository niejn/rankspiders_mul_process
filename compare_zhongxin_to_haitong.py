import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import xlsxwriter



# def cmp_citicsf_with(company='海通期货'):
#     con = 'sqlite:///exchange.sqlite'
#     engine = create_engine(con, echo=True)
#
#     sql_cmd = '''SELECT  report_date as 交易日, exchange AS 交易所, sum(中信期货成交) AS 中信期货成交,
#   sum({other_com}成交) AS {other_com}成交, sum(中信期货成交) - sum({other_com}成交) AS 差值
# FROM
#   (SELECT  zhongxin.report_date AS report_date, zhongxin.PARTICIPANTABBR1 AS 中信期货, zhongxin.CJ1 AS 中信期货成交,
#   zhongxin.instrumentid AS 合约代码, zhongxin.exchange AS exchage,
#   haitong.report_date, haitong.PARTICIPANTABBR1 AS {other_com}, haitong.CJ1 AS {other_com}成交, haitong.instrumentid, haitong.exchange
# FROM (SELECT report_date, PARTICIPANTABBR1, CJ1, exchange, instrumentid, productname FROM ranks
# WHERE PARTICIPANTABBR1 == '中信期货' AND VARIETY== 0  GROUP BY report_date, exchange, instrumentid) AS zhongxin JOIN
# (SELECT report_date, PARTICIPANTABBR1, CJ1, exchange, instrumentid, productname FROM ranks
# WHERE PARTICIPANTABBR1 == '{other_com}' AND VARIETY== 0 ORDER BY report_date, exchange, instrumentid) AS haitong
# WHERE zhongxin.report_date == haitong.report_date and zhongxin.instrumentid == haitong.instrumentid
#       and zhongxin.exchange == haitong.exchange) GROUP BY report_date, exchange ORDER BY exchange, report_date;
#
#       '''.format(other_com=company)
#     df = pd.read_sql_query(sql=(sql_cmd), con=engine)
#     print(df)
#     df['交易日'] = pd.to_datetime(df['交易日'])
#     df['交易日'] = df['交易日'].dt.strftime('%Y-%m-%d')
#     df.to_csv('中信期货{other_com}成交量对比.csv'.format(other_com=company), encoding='gbk', index=False)
#     return
from wsdl.全市场成交 import get_historydata_exchange


def citicsf_cmp_many_v2(con='sqlite:///exchange.sqlite', ):
    df_dict = get_historydata_exchange()
    engine = create_engine(con, echo=True)
    sql_cmd = '''SELECT report_date, PARTICIPANTABBR1, sum(CJ1), exchange FROM ranks
    WHERE PARTICIPANTABBR1 IN ('中信期货') AND VARIETY== 0  GROUP BY report_date, exchange,
      PARTICIPANTABBR1
    ORDER BY exchange, report_date, PARTICIPANTABBR1;'''
    citicsf_df = pd.read_sql_query(sql=(sql_cmd), con=engine)
    # print(citicsf_df.columns)
    # ['REPORT_DATE', 'PARTICIPANTABBR1', 'sum(CJ1)', 'EXCHANGE']
    # print(citicsf_df)
    sql_cmd = '''SELECT report_date, PARTICIPANTABBR1, sum(CJ1), exchange FROM ranks
        WHERE PARTICIPANTABBR1 LIKE '%海通期货%' AND VARIETY== 0  GROUP BY report_date, exchange,
          PARTICIPANTABBR1
        ORDER BY exchange, report_date, PARTICIPANTABBR1;'''
    df_haitong = pd.read_sql_query(sql=(sql_cmd), con=engine)
    result = pd.merge(citicsf_df, df_haitong, on=['REPORT_DATE', 'EXCHANGE'], suffixes=('_中信', '_海通'))
    result['中信-海通'] = result['sum(CJ1)_中信'] - result['sum(CJ1)_海通']
    result['REPORT_DATE'] = pd.to_datetime(result['REPORT_DATE'])
    result['REPORT_DATE'] = result['REPORT_DATE'].dt.strftime('%Y-%m-%d')
    print(result)

    result = citicsf_df
    # companys = [ '国投安信', '东证']
    companys = ['海通期货', '国投安信', '东证']
    for a_company in companys:
        sql_cmd = '''SELECT report_date, PARTICIPANTABBR1, sum(CJ1), exchange FROM ranks
                WHERE PARTICIPANTABBR1 LIKE '%{company}%' AND VARIETY== 0  GROUP BY report_date, exchange,
                  PARTICIPANTABBR1
                ORDER BY exchange, report_date, PARTICIPANTABBR1;'''.format(company=a_company)
        df_com = pd.read_sql_query(sql=(sql_cmd), con=engine)
        print(df_com)
        result = pd.merge(result, df_com, on=['REPORT_DATE', 'EXCHANGE'], suffixes=('', a_company))
        print(result)
        result['中信期货-{company}'.format(company=a_company)] = result['sum(CJ1)'] - result[
            'sum(CJ1){company}'.format(company=a_company)]
        print(result)

    result['REPORT_DATE'] = pd.to_datetime(result['REPORT_DATE'])
    result['REPORT_DATE'] = result['REPORT_DATE'].dt.strftime('%Y-%m-%d')
    print(result.columns)
    remain_cols = ['REPORT_DATE', 'EXCHANGE', 'sum(CJ1)',
                   'sum(CJ1)海通期货', 'sum(CJ1)国投安信', 'sum(CJ1)东证',
                   '中信期货-海通期货',
                   '中信期货-国投安信',
                   '中信期货-东证']
    result = result[remain_cols]
    # result['中信期货'] = result['sum(CJ1)']
    result.rename({'sum(CJ1)': '中信期货'}, axis='columns', inplace=True)
    result.rename(axis='columns', mapper=lambda x: x.replace('sum(CJ1)', ''), inplace=True)
    # result.rename()
    today = datetime.datetime.now()
    today_str = today.strftime('%Y%m%d')
    # result.to_csv('期货交易排名对比_{date}.csv'.format(date=today_str), encoding='gbk', index=False)
    excel_file_name = '期货交易排名对比_v1_{date}.xlsx'.format(date=today_str)
    # excel_file_name = './DailyStatement_{client_id}_{date}.xlsx'.format(client_id=client_id, date=report_date)
    writer = pd.ExcelWriter(excel_file_name, engine='xlsxwriter')
    workbook = writer.book
    cell_format1 = workbook.add_format({'border': 1})
    cell_format1.set_border()
    result.groupby(['EXCHANGE'])
    exchanges = result.groupby(['EXCHANGE', ])

    header_format = workbook.add_format({
        'bold': False,
        'text_wrap': False,

        'border': 1})
    header_format.set_align('center')
    header_format.set_align('vcenter')
    # header_format.set_border(1)
    header_format.set_font_size(12)
    header_format.set_shrink()
    cell_format = workbook.add_format({'text_wrap': False, })

    cell_format.set_align('center')
    cell_format.set_align('vcenter')

    for an_exchange in exchanges.groups:
        exchange_df = exchanges.get_group(an_exchange)
        # df.shape
        Count_Row = exchange_df.shape[0]  # gives number of row count
        Count_Col = exchange_df.shape[1]  # gives number of col count
        shape_val = exchange_df.shape
        # worksheet.set_column('A:DC',15,formater)
        exchange_df.to_excel(writer, sheet_name=an_exchange,

                             index=False
                             )
        beg_row = 1
        df_rows = exchange_df.shape[0]
        df_cols = exchange_df.shape[1]
        header_range = chr(65 + df_cols - 1)
        header_row = str(df_rows + 1)
        header_cmd_str = 'A1:{header_range}{row}'.format(header_range=header_range, row=header_row)
        worksheet = writer.sheets[an_exchange]
        worksheet.conditional_format(header_cmd_str, {'type': 'no_blanks', 'format': header_format})
        worksheet.set_column(header_cmd_str, 18, cell_format)
        print(exchange_df)
        crm_df = df_dict[an_exchange]
        print(crm_df)
        new_df = pd.merge(exchange_df, crm_df, how='inner', left_on=['REPORT_DATE', ], right_on=['time',])
        print(new_df)
        new_df['sum'] = new_df['sum'].astype(int)
        new_df['中信期货'] = new_df['中信期货'].astype(int)
        new_df['误差'] = new_df['中信期货']/new_df['sum']
        print(new_df)

    summarize_df = result.groupby('REPORT_DATE')[['REPORT_DATE', 'EXCHANGE', '中信期货', '海通期货',
                                                  '国投安信', '东证', '中信期货-海通期货',
                                                  '中信期货-国投安信', '中信期货-东证']].sum()
    print(summarize_df)
    summarize_df.reset_index(inplace=True)
    summarize_df.to_excel(writer, sheet_name='汇总', index=False)
    df_rows = exchange_df.shape[0]
    df_cols = exchange_df.shape[1]
    header_range = chr(65 + df_cols - 1)
    header_row = str(df_rows + 1)
    header_cmd_str = 'A1:{header_range}{row}'.format(header_range=header_range, row=header_row)
    worksheet = writer.sheets['汇总']
    worksheet.conditional_format(header_cmd_str, {'type': 'no_blanks', 'format': header_format})
    worksheet.set_column(header_cmd_str, 18, cell_format)

    workbook.close()
    writer.save()

    return


def citicsf_cmp_many_v1(con = 'sqlite:///exchange.sqlite',):
    engine = create_engine(con, echo=True)
    sql_cmd = '''SELECT report_date, PARTICIPANTABBR1, sum(CJ1), exchange FROM ranks
    WHERE PARTICIPANTABBR1 IN ('中信期货') AND VARIETY== 0  GROUP BY report_date, exchange,
      PARTICIPANTABBR1
    ORDER BY exchange, report_date, PARTICIPANTABBR1;'''
    citicsf_df = pd.read_sql_query(sql=(sql_cmd), con=engine)
    # print(citicsf_df.columns)
    # ['REPORT_DATE', 'PARTICIPANTABBR1', 'sum(CJ1)', 'EXCHANGE']
    # print(citicsf_df)
    sql_cmd = '''SELECT report_date, PARTICIPANTABBR1, sum(CJ1), exchange FROM ranks
        WHERE PARTICIPANTABBR1 LIKE '%海通期货%' AND VARIETY== 0  GROUP BY report_date, exchange,
          PARTICIPANTABBR1
        ORDER BY exchange, report_date, PARTICIPANTABBR1;'''
    df_haitong = pd.read_sql_query(sql=(sql_cmd), con=engine)
    result = pd.merge(citicsf_df, df_haitong, on=['REPORT_DATE', 'EXCHANGE'], suffixes=('_中信', '_海通'))
    result['中信-海通'] = result['sum(CJ1)_中信'] - result['sum(CJ1)_海通']
    result['REPORT_DATE'] = pd.to_datetime(result['REPORT_DATE'])
    result['REPORT_DATE'] = result['REPORT_DATE'].dt.strftime('%Y-%m-%d')
    print(result)

    result = citicsf_df
    # companys = [ '国投安信', '东证']
    companys = ['海通期货', '国投安信', '东证']
    for a_company in companys:
        sql_cmd = '''SELECT report_date, PARTICIPANTABBR1, sum(CJ1), exchange FROM ranks
                WHERE PARTICIPANTABBR1 LIKE '%{company}%' AND VARIETY== 0  GROUP BY report_date, exchange,
                  PARTICIPANTABBR1
                ORDER BY exchange, report_date, PARTICIPANTABBR1;'''.format(company=a_company)
        df_com = pd.read_sql_query(sql=(sql_cmd), con=engine)
        print(df_com)
        result = pd.merge(result, df_com, on=['REPORT_DATE', 'EXCHANGE'], suffixes=('', a_company))
        print(result)
        result['中信期货-{company}'.format(company=a_company)] = result['sum(CJ1)'] - result['sum(CJ1){company}'.format(company=a_company)]
        print(result)

    result['REPORT_DATE'] = pd.to_datetime(result['REPORT_DATE'])
    result['REPORT_DATE'] = result['REPORT_DATE'].dt.strftime('%Y-%m-%d')
    print(result.columns)
    remain_cols = ['REPORT_DATE', 'EXCHANGE', 'sum(CJ1)',
     'sum(CJ1)海通期货','sum(CJ1)国投安信','sum(CJ1)东证',
                   '中信期货-海通期货',
       '中信期货-国投安信',
       '中信期货-东证']
    result =result[remain_cols]
    # result['中信期货'] = result['sum(CJ1)']
    result.rename({'sum(CJ1)':'中信期货'}, axis='columns', inplace=True)
    result.rename(axis='columns', mapper=lambda x: x.replace('sum(CJ1)', ''), inplace=True)
    # result.rename()
    today = datetime.datetime.now()
    today_str = today.strftime('%Y%m%d')
    # result.to_csv('期货交易排名对比_{date}.csv'.format(date=today_str), encoding='gbk', index=False)
    excel_file_name = '期货交易排名对比_{date}.xlsx'.format(date=today_str)
    # excel_file_name = './DailyStatement_{client_id}_{date}.xlsx'.format(client_id=client_id, date=report_date)
    writer = pd.ExcelWriter(excel_file_name, engine='xlsxwriter')
    workbook = writer.book
    cell_format1 = workbook.add_format({'border':1})
    cell_format1.set_border()
    result.groupby(['EXCHANGE'])
    exchanges = result.groupby(['EXCHANGE', ])

    header_format = workbook.add_format({
        'bold': False,
        'text_wrap': False,

        'border': 1})
    header_format.set_align('center')
    header_format.set_align('vcenter')
    # header_format.set_border(1)
    header_format.set_font_size(12)
    header_format.set_shrink()
    cell_format = workbook.add_format({'text_wrap': False,})

    cell_format.set_align('center')
    cell_format.set_align('vcenter')

    for an_exchange in exchanges.groups:
        exchange_df = exchanges.get_group(an_exchange)
        # df.shape
        Count_Row = exchange_df.shape[0]  # gives number of row count
        Count_Col = exchange_df.shape[1]  # gives number of col count
        shape_val = exchange_df.shape
        # worksheet.set_column('A:DC',15,formater)
        exchange_df.to_excel(writer, sheet_name=an_exchange,

                      index=False
                      )
        beg_row = 1
        df_rows = exchange_df.shape[0]
        df_cols = exchange_df.shape[1]
        header_range = chr(65 + df_cols - 1)
        header_row = str(df_rows+1)
        header_cmd_str = 'A1:{header_range}{row}'.format(header_range=header_range, row=header_row)
        worksheet = writer.sheets[an_exchange]
        worksheet.conditional_format(header_cmd_str, {'type': 'no_blanks', 'format': header_format})
        worksheet.set_column(header_cmd_str, 18, cell_format)

    summarize_df = result.groupby('REPORT_DATE')[['REPORT_DATE', 'EXCHANGE', '中信期货', '海通期货',
                                                  '国投安信', '东证', '中信期货-海通期货',
                                                    '中信期货-国投安信', '中信期货-东证']].sum()
    print(summarize_df)
    summarize_df.reset_index(inplace=True)
    summarize_df.to_excel(writer, sheet_name='汇总', index=False )
    df_rows = exchange_df.shape[0]
    df_cols = exchange_df.shape[1]
    header_range = chr(65 + df_cols - 1)
    header_row = str(df_rows + 1)
    header_cmd_str = 'A1:{header_range}{row}'.format(header_range=header_range, row=header_row)
    worksheet = writer.sheets['汇总']
    worksheet.conditional_format(header_cmd_str, {'type': 'no_blanks', 'format': header_format})
    worksheet.set_column(header_cmd_str, 18, cell_format)

    workbook.close()
    writer.save()




    return


'''# Get the xlsxwriter workbook and worksheet objects.
workbook  = writer.book
worksheet = writer.sheets['Sheet1']

# Add some cell formats.
format1 = workbook.add_format({'num_format': '#,##0.00'})
format2 = workbook.add_format({'num_format': '0%'})

# Note: It isn't possible to format any cells that already have a format such
# as the index or headers or any cells that contain dates or datetimes.

# Set the column width and format.
worksheet.set_column('B:B', 18, format1)

# Set the format but not the column width.
worksheet.set_column('C:C', None, format2)

# Close the Pandas Excel writer and output the Excel file.
writer.save()'

#############################################################################
#
# An example of converting a Pandas dataframe to an xlsx file
# with a user defined header format.
#
# Copyright 2013-2018, John McNamara, jmcnamara@cpan.org
#

import pandas as pd

# Create a Pandas dataframe from some data.
data = [10, 20, 30, 40, 50, 60]
df = pd.DataFrame({'Heading': data,
                   'Longer heading that should be wrapped' : data})

# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter("pandas_header_format.xlsx", engine='xlsxwriter')

# Convert the dataframe to an XlsxWriter Excel object. Note that we turn off
# the default header and skip one row to allow us to insert a user defined
# header.
df.to_excel(writer, sheet_name='Sheet1', startrow=1, header=False)

# Get the xlsxwriter workbook and worksheet objects.
workbook  = writer.book
worksheet = writer.sheets['Sheet1']

# Add a header format.
header_format = workbook.add_format({
    'bold': True,
    'text_wrap': True,
    'valign': 'top',
    'fg_color': '#D7E4BC',
    'border': 1})

# Write the column headers with the defined format.
for col_num, value in enumerate(df.columns.values):
    worksheet.write(0, col_num + 1, value, header_format)

# Close the Pandas Excel writer and output the Excel file.
writer.save()

'''


def main():
    con = 'sqlite:///exchange.sqlite'
    citicsf_cmp_many_v2()
    # citicsf_cmp_many_v1()
    # citicsf_cmp_many()

    # cmp_citicsf_with(company='东证期货')
    return

if __name__ == '__main__':
    main()