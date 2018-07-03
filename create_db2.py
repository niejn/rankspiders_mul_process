#encoding:utf-8
from sqlalchemy import MetaData, Boolean
from datetime import datetime
from sqlalchemy import Table, Column, Integer, Numeric, String, ForeignKey, DateTime
from sqlalchemy import create_engine

# Column('date', Integer(), primary_key=True),
metadata = MetaData()
# engine = create_engine('sqlite:///:memory:')
engine = create_engine('sqlite:///exchange.sqlite')

# 营业部 department
# 业务员 salesman
# 客户姓名	acc_name
# 客户号	acc_id
# 合约品种	future_id
# 成交手数 trading_volume
# 成交金额 turnover
# 日期 date

# Trades = Table('trades', metadata,
#     Column('id', Integer, primary_key=True, autoincrement=True),
#     Column('acc_id', Integer(), index=True),
#     Column('date', Integer()),
#     Column('acc_name', String(50)),
#     Column('trading_volume', Integer()),
#     Column('turnover', Integer()),
#     Column('future_id', String(50)),
#     Column('department', String(50)),
#     Column('salesman', String(50)),
#
# )

ranks = Table('ranks',
                  metadata,
                  Column('id', Integer(), primary_key=True, autoincrement=True),
                  Column('rank', Integer(), comment='名次'),
                  Column('CJ1', Integer(), comment='成交量'),
                  Column('CJ2', Integer(), comment='持买单量'),
                  Column('CJ3', Integer(), comment='持卖单量'),
                  Column('PARTICIPANTABBR3',  String(50), comment='期货公司会员简称3'),
                  Column('PARTICIPANTABBR2',  String(50), comment='期货公司会员简称2'),
                  Column('PARTICIPANTABBR1',  String(50), comment='期货公司会员简称1'),
                  Column('CJ3_CHG', Integer(), comment='比上交易日增减3'),
                  Column('CJ2_CHG', Integer(), comment='比上交易日增减2'),
                  Column('CJ1_CHG', Integer(), comment='比上交易日增减1'),
                  Column('instrumentid', String(50), comment='instrumentid'),
                  Column('productname', String(50), comment='productname'),
                  Column('productsortno', Integer(), comment='productsortno'),
                  Column('participantid1', Integer(), comment='participantid1'),
                  Column('participantid2', Integer(), comment='participantid2'),
                  Column('participantid3', Integer(), comment='participantid3'),
                  Column('variety', Boolean(), comment='variety or instrument'),
                  Column('date', Integer(), comment='participantid3'),
                  Column(DateTime, default=datetime.utcnow)
                  )


#  variety
# Account	Name	Trading Volume		increase	growth_rate	Corporate_trade_vol	Corporate occupation ratio	market_trade_vol	marketoccupation ratio
# Base.metadata.create_all()
ans = metadata.create_all(engine)
print(ans)