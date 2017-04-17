#!/usr/bin/env python
#coding=utf-8


'''检查数据库中数据是否和csv文件一致
已实现：
1. 检查合约
2. 检查持仓
3. 检查保证金率
4. 检查资金
5. 检查手续费率
6. 检查hashaddress


未实现：
1. fpga中资金，symboltable的一些值
2. 
'''

import csv
import MySQLdb
import os
import initData
import time
import getMarRate
import logging


#比较csv文件中和mysql中的合约名是否一致
def compareInst(list1,list2):
	if set(list1) == set(list2):
		logging.debug('Check instrument : CSV file match SQL data !')
		return 1
	elif (set(list1) - set(list2)):
		logging.error('CSV has instrument: %s  not load' % (set(list1) - set(list2)))
		return 0
	elif (set(list2) - set(list1)):
		logging.error('CSV has no instrument:%s, but load' % (set(list2) - set(list1)))
		return 0
#比较合约的详细信息是否一致
def compareInstDtl():
	list1,dict1 = initData.getInstFroCsv()
	list2,dict2 = initData.getInstFroSql()
	total_count = 0
	error_count = 0
	if compareInst(list1,list2):
		for one in list1:
			if dict1[one] == list(dict2[one]):
				logging.debug("Instrument : %s compare detail correct " % one)
			else:
				logging.error("Instrument : %s compare detail error " % one)
				error_count += 1
				return 0 
			total_count += 1
	else:
		logging.error('compare  error !')
	logging.debug('Check Instrument finished,Total count: %d, Error count : %d' %(total_count, error_count))

#持仓检查
def checkPosition():
	cur_investor = initData.getInvestorFroSql()
	cur_position = initData.connDB()
	for investorid in cur_investor:
		position = initData.getPosition(investorid)
		error_count = 0
		logging.debug('Check investor: %s position begin' % investorid)
		for symbol in position:
			cur_position.execute('select bought_position,sold_position from position where investor_id = %s and instrument_id = "%s" and (sold_position > 0 or bought_position > 0);' % (investorid,symbol))
			text1 = cur_position.fetchone() 
#			print(text1)
#			if text1 == None:
#				print("mysql do not have this instrument:",symbol)
#				continue
#			print(text1)
#			print(position[symbol])
			if text1 is None and position is None:
				logging.debug("Investor: %s,Instrument %s no position " %(investorid,symbols))
				continue
			try:			
				if list(text1) == position[symbol][:2]:
					logging.debug("Investor: %s,Instrument %s bought %d sold %d,check position correct" % (investorid,symbol,text1[0],text1[1]))
				else:
					error_count += 1
					logging.error("Investor: %s,Instrument %s, check position error !" % (investorid,symbol))
					print(investorid,symbol,position[symbol][0],position[symbol][1],text1[0],text1[1])
					logging.error("Investor: %s,Instrument %s,csv bought %d sold %d,sql bought %d sold %d" %(investorid,symbol,position[symbol][0],position[symbol][1],text1[0],text1[1]))
			except Exception as e:
				logging.error(e)
				logging.error("Investor:%s check position error" % investorid)
		if error_count == 0:
			logging.debug("Investor: %s,check position finished, correct !" % investorid)
		else:
			logging.error("Investor: %s position check position finished, error count : %d " % (investorid,error_count))
#检查保证金率
def checkMarginRate():
	investors = initData.getInvestorFroSql()
	instruments = initData.getInstFroCsv()[0]
	cur = initData.connDB()
	for investorid in investors:
		logging.debug("Check marginrate begin,investor: %s" % investorid)
		for instrumentid in instruments:
			csvMarRate = getMarRate.countMarate(instrumentid,investorid)
			logging.debug('Instrument:%s Get marginrate from csv,the values is %s' % (instrumentid ,csvMarRate))

			cur.execute('SELECT long_margin_ratio_by_money FROM instrument_margin_rate where investor_id = %s and instrument_id = \'%s\';' %(investorid,instrumentid))
			try:
				sqlMarRate = cur.fetchone()[0]
			except Exception as e:
				logging.error(e)
				continue
			logging.debug('Instrument:%s Get marginrate from mysql,the values is %s' % (instrumentid ,sqlMarRate))
			if float('%.4f' % csvMarRate) == float('%.4f' % sqlMarRate):
				logging.debug('Investot:%s Instrument:%s Marginrate match' % (investorid,instrumentid))
			else:
				logging.error('Instrument:%s Marginrate is not match' % instrumentid)

#检查投资者资金账户的静态权益，保证金，交割保证金，风险度是否正确
def checkAccount():
	investors = initData.getInvestorFroSql()
	for investorid in investors:
		csv_margin = initData.getMargin(investorid)
		csv_staticright,csv_deliverymargin = initData.getStaticRightForCsv(investorid)
		logging.debug("Investor: %s,data from csv file staticright:%f margin:%f deliverymargin:%f" %(investorid,csv_staticright,csv_margin,csv_deliverymargin))
		sql_data = initData.getAccountFromDB(investorid)
		sql_staticright,sql_margin,sql_deliverymargin,sql_risk = sql_data
		logging.debug("Investor: %s,data from mysql staticright:%f margin:%f deliverymargin:%f" %(investorid,sql_staticright,sql_margin,sql_deliverymargin))
		if csv_margin == sql_margin and csv_staticright == sql_staticright and csv_deliverymargin == sql_deliverymargin:
			logging.debug("Investor: %s check staticright and margin correct" % investorid)
			if csv_staticright <= 0 :
				risk_csv = 100
				logging.debug("Investor: %s data from csv file risk is 100" % investorid)
			else:	
				risk_csv = float((csv_margin + csv_deliverymargin)/csv_staticright*100)
				logging.debug("Investor: %s data from csv file risk is %f" % (investorid,risk_csv))
			logging.debug("Investor: %s data from mysql file risk is %f" % (investorid,sql_risk))
			if float('%.2f' % risk_csv) == float('%.2f' %sql_risk):
				logging.debug('Investor: %s check risk correct ' % investorid)
			else:
				logging.error('Investor: %s check risk Error ' % investorid)
		else:
			logging.error('Investor: %s check staticright or margin or deliverymargin Error ' % investorid)
	
def checkComm():
	investors = initData.getInvestorFroSql()
	instruments = initData.getInstFroCsv()[0]
	cur = initData.connDB()
	for investorid in investors:
		logging.debug("Check commissionrate begin,investor: %s" % investorid)
		for instrumentid in instruments:
			csv_comm = initData.getCommRate(instrumentid,investorid)
#			print('investor: %s instrument: %s commissionrate: %s' %(investorid,instrumentid,csv_comm))
			cur.execute('SELECT open_ratio_by_money,open_ratio_by_volume,close_ratio_by_money,close_ratio_by_volume,close_today_ratio_by_money,close_today_ratio_by_volume FROM test_shfe.commission_rate where instrument_id = "%s" and investor_id = %s;' %(instrumentid,investorid))
			sql_comm = list(cur.fetchone())
			if csv_comm == sql_comm:
				logging.debug("Investor:%s instrument: %s commissionrate match correct" %(investorid,instrumentid))
				
			else:
				logging.error("Investor:%s instrument: %s commissionrate error" %(investorid,instrumentid))
				logging.error("csv data:  sql data:" %(csv_comm,sql_comm))
	
def checksymboltable():
	cur = initData.connDB()
	investor={}
	cur.execute('select investor_id,session from investor_session where status=1;')
	for row in cur.fetchall():
		if not row:
			logging.error("no investor active")
#			print("no investor active")
			break
		try:
			instruments = initData.getInstFroSql()[0]
		except Exception as e:
			logging.error(e)
			logging.error("database has no instrument...")
#			print("error: the database has no instrument ")
			continue
		for instrument in instruments:
			var = os.popen('./checksymbol.sh %s %s' %(row[1],instrument)).read()
			exec(var)
#			print(row[0],row[1],instrument)
			list_values = [client_symbol,symbol_address,open_fee,close_fee,old_bought_position,old_sold_position,contract_multiple,margin_ration,fee_type]
			logging.debug("Investor:%s instrument:%s the values from fpga is: " %(row[0],instrument))
			logging.debug(list_values)
			cur.execute('SELECT * FROM test_shfe.symbol_table_address where session=%s and instrument_id="%s";'%(row[1],instrument))
#			print(cur.fetchone()[4])
			sql_symbol_address = cur.fetchone()[4]
#			print(client_symbol,symbol_address,open_fee,close_fee,old_bought_position,old_sold_position,contract_multiple,margin_ration,fee_type)
			if symbol_address != sql_symbol_address:
				logging.debug(row[0],row[1],instrument)
				logging.debug("fpga:%s  sql:%s " %(symbol_address,sql_symbol_address))
			else:
				logging.debug("symbol address compare correct")
	

if __name__ == '__main__':
	logging.basicConfig(level = logging.DEBUG,filename='file.log',format='%(asctime)s %(levelname)s %(message)s',filemode='w')
	compareInstDtl()
	checkPosition()
	checkMarginRate()
	checkAccount()
	checkComm()
	checksymboltable()
