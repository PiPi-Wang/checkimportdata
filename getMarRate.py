#/usr/bin/env python
#coding=utf-8

import sys
import csv
import time
import initData


#根据不同的range条件去搜索
def Range(dict_rang,instrumentid,investorid,range,pid):
	IMR1,IMR2 = 0,0
	if dict_rang['InvestorID'] == str(investorid):
		if dict_rang['InvestorRange'] == str(range) and dict_rang['HedgeFlag'] == str(1):
			if  dict_rang['InstrumentID'] == str(instrumentid) :
#			print (dict_rang['LongMarginRatioByMoney'],'aa')
				IMR1 = float(dict_rang['LongMarginRatioByMoney'])
			elif  dict_rang['InstrumentID'] == pid :
#				print (dict_rang['LongMarginRatioByMoney'],'bb')
				IMR2 = float(dict_rang['LongMarginRatioByMoney'])
#	print dict_rang['InvestorID'],investorid,dict_rang['InvestorRange'],range
	return (IMR1,IMR2)


#输入合约ID，投资者id，mid，investor文件，instrumentmarginrate文件，instrument文件，得到marginrate
def getIMR(instrumentid,investorid,mid,investor,instrumentmarginrate,instrument):
	key = 'p'
	IMR1,IMR2,IMR_TEMP,IMR,isrelative = 0,0,0,0,0
	i = 0
	mid = mid
	PID = initData.InitalProductID(instrumentid)
	with open(instrumentmarginrate, 'r') as f:
		reader = csv.DictReader(f)
		
		for row in reader: #使用的41008143对此测试，al1706
			IMR1,IMR2 = Range(row,instrumentid,investorid,3,PID)
			if IMR1 != 0 :
				relative = row['IsRelative']
				IMR = IMR1
				return (IMR,relative)
			elif IMR2 != 0:
				IMR_TEMP = IMR2	
				relative = row['IsRelative']
		if IMR_TEMP != 0 :
			IMR = IMR_TEMP
			return (IMR,relative)

		f.seek(0,0)
		for row in reader:
			investorid = mid
			IMR1,IMR2 = Range(row,instrumentid,investorid,2,PID)
			if IMR1 != 0 :
#				print(row)
				IMR = IMR1
				relative = row['IsRelative']
				return (IMR,relative)
			elif IMR2 != 0:
				IMR_TEMP = IMR2	
				relative = row['IsRelative']
		if IMR_TEMP != 0 :
			IMR = IMR_TEMP
			return (IMR,relative)

		f.seek(0,0)
		for row in reader:
			investorid = '00000000'
			IMR1,IMR2 = Range(row,instrumentid,investorid,1,PID)
			if IMR1 != 0 :
#				print(row)
				relative = row['IsRelative']
				IMR = IMR1
				return (IMR,relative)
			elif IMR2 != 0:
				relative = row['IsRelative']
				IMR_TEMP = IMR2	
		if IMR_TEMP != 0 :
			IMR = IMR_TEMP
			return (IMR,relative)

	return (IMR,isrelative)

def getMra(result):
	if result == 'no_data':
		result = 0
	else:
		result = result
	return result

#获取exchangemarginrate
def getExcMR(instrumentid,input_file):
	PID = initData.InitalProductID(instrumentid)
	with open(input_file,'r') as f:
		file = csv.DictReader(f)
		for row in file:
			if row['HedgeFlag'] == '1' and row['InstrumentID'] == str(instrumentid):
#				print(float(row['LongMarginRatioByMoney']))
				return float(row['LongMarginRatioByMoney'])
		f.seek(0,0)
		for row in file:
			if row['HedgeFlag'] == '1' and row['InstrumentID'] == PID:
#				print(float(row['LongMarginRatioByMoney']))
				return float(row['LongMarginRatioByMoney'])
	return 0
#获取investorid对应的mid
def getMid(investor,investorid):
	with open(investor,'r') as investor_f:
		for line in csv.DictReader(investor_f):
			if line['InvestorID'] == str(investorid):
				mid = line['MarginModelID']
				return mid
		
def countMarate(instrumentid,investorid):
	instrumentmarginrate_csv = 't_InstrumentMarginRate.csv'
	instrumentmarginrateadjust_csv = 't_InstrumentMarginRateAdjust.csv'
	exchangemarginrate = 't_ExchangeMarginRate.csv'
	exchangemarginrateadjust = 't_ExchangeMarginRateAdjust.csv'	
	investor = 't_Investor.csv'
	instrument = 't_Instrument.csv'
	mid = getMid(investor,investorid)
	IMR,isrelative = getIMR(instrumentid,investorid,mid,investor,instrumentmarginrate_csv,instrument)
#	print(IMR)
	IMRAdjust,isrelative1 = getIMR(instrumentid,investorid,mid,investor,instrumentmarginrateadjust_csv,instrument)	
#	print(IMRAdjust)
	ExcMR = getExcMR(instrumentid,exchangemarginrate)
#	print(ExcMR)
	ExcMRAdjust = getExcMR(instrumentid,exchangemarginrateadjust)
#	print(ExcMRAdjust)
	if str(isrelative) == '0' :
		return max(IMR + IMRAdjust + ExcMRAdjust,ExcMR + ExcMRAdjust)
	if str(isrelative) == '1' :
		return (IMR + IMRAdjust + ExcMRAdjust + ExcMR)
	
		
if __name__ == '__main__':
	print(countMarate('ag1705','41008760'))
#	print(countMarate('ag1712','41008757'))
