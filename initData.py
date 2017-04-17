#!/usr/bin/env python
#coding=utf-8


import csv
import MySQLdb
import sys
import getMarRate


#建立数据库连接，返回一个游标对象
def connDB():
	host = 'cd05'
	user = 'root'
	passwd = 'Accelecom.123'
	db = 'test'
	conn = MySQLdb.connect(host,user,passwd,db)
	cur = conn.cursor()
	return cur

#从数据库中获取状态为活跃的投资者，返回投资者组成的列表
def getInvestorFroSql():
	Investor_list = []
	cur = connDB()
	count = cur.execute('select investor_id from investor where status = 1;')
	for one in range(count):
		Investor_list.append(cur.fetchone()[0])
	return Investor_list

#从csv文件获取合约，返回合约名的列表，合约名为key，一些值组成的列表为value的字典
def getInstFroCsv():
	instrument = 't_Instrument.csv'
	exchangeid = 'SHFE'
	istrading,productclass,instlifephase = 1,1,1
	list_csv = []
	dict_csv = {}
	with open(instrument,'r') as f:
		rows = csv.DictReader(f)
		for row in rows:
			if row['ExchangeID'] == str(exchangeid):
				if row['IsTrading'] == str(istrading) and row['ProductClass'] == str(productclass) and row['InstLifePhase'] == str(instlifephase):
#					print('The %d instrument : %s' %(count,row['InstrumentID']))
					list_csv.append(row['InstrumentID'])
					dict_csv[row['InstrumentID']] = [row['InstrumentID'],row['ExchangeID'],row['ProductID'],int(row['DeliveryYear']),int(row['DeliveryMonth']),int(row['MaxMarketOrderVolume']),int(row['MinMarketOrderVolume']),int(row['VolumeMultiple']),float(row['PriceTick'])]
	return list_csv,dict_csv

#输入投资者ID，取出CSV文件中该投资者以合约名为key，买仓手数，卖仓手数，结算价格列表为值的字典
def getPosition(investorid):
	investorposition = 't_InvestorPositionDtl.csv'
	exchangeid = 'SHFE'
	Inst_dict = {}
	Inst_list = [0,0,0]
	with open(investorposition,'r') as file:
		rows = csv.DictReader(file)
		for row in rows:
			
			if row['InvestorID'] == str(investorid) and row['ExchangeID'] == str(exchangeid) and row['HedgeFlag'] == '1':
				if Inst_dict.get(row['InstrumentID']):
					if row['Direction'] == '0':
						Inst_list[0] = int(Inst_list[0]) + int(row['Volume'])
					elif row['Direction'] == '1':
						Inst_list[1] = int(Inst_list[1]) + int(row['Volume'])
					Inst_dict[row['InstrumentID']] = Inst_list
				else:
					Inst_list = [0,0,0]
					if row['Direction'] == '0':
						Inst_list[0] = int(row['Volume'])
					elif row['Direction'] == '1':
						Inst_list[1] = int(row['Volume'])
					Inst_list[2] = float(row['LastSettlementPrice'])
					Inst_dict[row['InstrumentID']] = Inst_list
	return Inst_dict

#取mysql中表instrument中的合约名作为列表
def getInstFroSql():
	list_sql = []
	dict_sql = {}
	cur = connDB()
	line_count = cur.execute('select instrument_id,exchange_id,product_id,delivery_year,delivery_month,max_market_order_volume,min_market_order_volume,volume_multiple,price_tick from instrument where date(insert_time) = current_date() ;')
	for one in range(line_count):
		row = cur.fetchone()
		list_sql.append(row[0])
		dict_sql[row[0]] = row
	return list_sql,dict_sql

#从csv文件获取合约乘数
def InitalVolumeMultipa(instrumentid):
	Instrument = 't_Instrument.csv'
	with open(Instrument,'r') as csv_instrument:
		text = csv.DictReader(csv_instrument)
		for instrument in text:
			if instrument['InstrumentID'] == instrumentid:
				VolMult = int(instrument['VolumeMultiple'])
				break
	return VolMult
	
#从csv文件获取ProductID
def InitalProductID(instrumentid):
	Instrument = 't_Instrument.csv'
	with open(Instrument,'r') as csv_instrument:
		text = csv.DictReader(csv_instrument)
		for instrument in text:
			if instrument['InstrumentID'] == instrumentid:
				PID = instrument['ProductID']
				break
	return PID

#从CSV文件获取静态权益、交割保证金
def getStaticRightForCsv(investorid):
	TradingAccount = 't_TradingAccount.csv'
	with open(TradingAccount,'r')as f:
		rows = csv.DictReader(f)
		for row in rows:
			if row['AccountID'] == str(investorid):
				return (float(row['PreBalance']),float(row['DeliveryMargin']))

#从数据库获取静态权益，
def getAccountFromDB(investorid):
	cur = connDB()
	cur.execute("select static_rights,used_margin,delivery_margin,risk from trading_data_current where investor_id = %s;" % investorid)
	values = cur.fetchone()
	sql_staticright = float('%.2f' % values[0])
	sql_used_margin = float('%.2f' % values[1])
	sql_deliverymargin = float('%.2f' % values[2])
	risk = float('%.2f' % values[3])
	return (sql_staticright,sql_used_margin,sql_deliverymargin,risk)

def getMargin(investorid):
    TotalMargin = 0
    ExcMarRate = 't_ExchangeMarginRate.csv'
    InsMarRate = 't_InstrumentMarginRate.csv'
    Position = getPosition(investorid)
    for one in Position:
        MarginRate = getMarRate.countMarate(one,investorid)
        VolMult = InitalVolumeMultipa(one)
        Position[one] = (int(Position[one][0])+int(Position[one][1]))*Position[one][2]*MarginRate*VolMult
        TotalMargin += Position[one]
#    print(" The Account:%s,Margin:%s" %(investorid,TotalMargin))
    return float('%.2f' % TotalMargin)

	
#获取investorid的instrumentid的手续费率，返回列表[float(row['OpenRatioByMoney']),float(row['OpenRatioByVolume']),float(row['CloseRatioByMoney']),float(row['CloseRatioByVolume']),float(row['CloseTodayRatioByMoney']),float(row['CloseTodayRatioByVolume'])]
def getCommRate(instrumentid,investorid):
#	getFile()
	comm_list2,comm_list3 = [],[]
	investor = 't_Investor.csv'
	instrument = 't_Instrument.csv'
	instrumentcommissionrate = 't_InstrumentCommissionRate.csv'
	mid = getMarRate.getMid(investor,investorid)
	instrument_dict = getInstFroCsv()[1]
	productid = instrument_dict[instrumentid][2]
	with open(instrumentcommissionrate,'r') as f:
		rows = csv.DictReader(f)
		for row in rows:
			if row['InstrumentID'] == productid:
				if row['InvestorID'] == investorid and row['InvestorRange'] == '3':
					comm_list = [float(row['OpenRatioByMoney']),float(row['OpenRatioByVolume']),float(row['CloseRatioByMoney']),float(row['CloseRatioByVolume']),float(row['CloseTodayRatioByMoney']),float(row['CloseTodayRatioByVolume'])]
					return comm_list
				elif row['InvestorID'] == mid and row['InvestorRange'] == '2':
					comm_list2 = [float(row['OpenRatioByMoney']),float(row['OpenRatioByVolume']),float(row['CloseRatioByMoney']),float(row['CloseRatioByVolume']),float(row['CloseTodayRatioByMoney']),float(row['CloseTodayRatioByVolume'])]
					
				elif row['InvestorID'] == '00000000' and row['InvestorRange'] == '1':
					comm_list3 = [float(row['OpenRatioByMoney']),float(row['OpenRatioByVolume']),float(row['CloseRatioByMoney']),float(row['CloseRatioByVolume']),float(row['CloseTodayRatioByMoney']),float(row['CloseTodayRatioByVolume'])]
	if comm_list2:
		return comm_list2
	elif comm_list3:
		return comm_list3
	else:
		print('error')
		logging.error('CommissionRate: instruement %s has no commissionrate' % instrumentid)


if __name__ == '__main__':
	for one in getInvestorFroSql():
		print(one,getMargin(one))
