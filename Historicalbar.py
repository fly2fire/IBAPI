from ibapi.wrapper import EWrapper
from ibapi import utils
from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.common import BarData
from Scripts.mycontracts import createContractObject
import pandas as pd
import dbconnect as db
from datetime import datetime, timedelta

import time

dbconn = db.connectdb()
dbconn.pgconnect()
requestcounter = 0
class App(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, wrapper=self)
        self.tickerlist = ['ALQ', 'ALU', 'AWC', 'ANN', 'APA', 'ALX', 'AST', 'BOQ', 'BPT', 'BEN', 'BSL', 'BLD', 'BXB',
                           'CAR', 'CHC', 'CIM', 'CWY', 'CCL', 'COL', 'CBA', 'CPU', 'CWN', 'DXS', 'DMP', 'DOW', 'EVN',
                           'FLT', 'GMG', 'GPT', 'ILU', 'IPL', 'IAG', 'JHX', 'JBH', 'LLC', 'LNK', 'MQG', 'MFG', 'MPL',
                           'MGR', 'NAB', 'NCM', 'NHF', 'NEC', 'NST', 'OSH', 'ORI']#'ORG', 'ORA', 'OZL', 'QBE', 'QUB',
                           # 'RHC', 'REA', 'RWC', 'RMD', 'RIO', 'STO', 'SCG', 'SEK', 'SHL', 'SOL', 'S32', 'SKI', 'SGP',
                           # 'SUN', 'SYD', 'TAH', 'TLS', 'A2M', 'SGR', 'TPM', 'TCL', 'TWE', 'URW', 'VCX', 'VUK', 'WES',
                           # 'WBC', 'WHC', 'WPL', 'WOW', 'WOR', 'XRO']
        self.contracts = createContractObject(self.tickerlist)
        self.mydict = {}
        self.contdict = {}
        self.donelist = []
        self.lasttime = time.time()
        self.requestcounter = 0

    def nextValidId(self, orderId:int):
        print("setting nextValidOrderId: ", orderId)
        self.nextValidOrderId = orderId
        for i,j in zip([str(z) for z in range(self.nextValidOrderId,self.nextValidOrderId+len(self.contracts))],self.tickerlist):
            self.mydict[i]=j
        self.start()

    def historicalData(self, reqId:int, bar: BarData):
        self.bardata = (self.mydict[str(reqId)],bar.date,bar.open,bar.high,bar.low, bar.close,bar.volume,bar.average, bar.barCount)
        dbvalues = (str(self.bardata))
        print('inserting '+ dbvalues)
        query = "INSERT INTO fivesecondbar VALUES " + dbvalues
        dbconn.pgquery(dbconn.conn,query,None)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        print("HistoricalDataEnd. ReqId:", reqId, "from", start, "to", end)
        timestart = datetime.strptime(start,"%Y%m%d %H:%M:%S")
        # This is the ultimate start date of request
        if timestart < datetime(2019,4,16):
            self.donelist.append(reqId)
        # if start date less than 10am need to skip the previous night
        if timestart.hour < 10:
            print(start)
            tempstart  = timestart - timedelta(days = 1)
            #start at close previous day
            tempstart =  tempstart.replace(hour=16,minute=11)
            start = tempstart.strftime('%Y%m%d %H:%M:%S')
        if reqId not in self.donelist:
            self.requestcounter +=1
            self.throttle()
            print('getting data ending ' + str(start))
            self.reqHistoricalData(reqId, self.contdict[reqId], start,'3600 S',
                                   '5 secs', 'TRADES',0, 1, False,[])

    def error(self, reqId, errorCode, errorString):
        print("Error. Id: " , reqId, " Code: " , errorCode , " Msg: " , errorString)

    def throttle(self):
        print(self.requestcounter)
        self.waittime= self.lasttime + 61 - time.time()
        if self.requestcounter%6 ==0 and self.waittime>0:
            print('waiting for ' + str(self.waittime) + ' seconds')
            time.sleep(self.waittime)
        self.lasttime = time.time()

    def start(self):
        for i in range(len(self.contracts)):
            #fill dictionary to map reqid to contract object
            self.contdict[self.nextValidOrderId + i] = self.contracts[i]
            print(self.contdict[self.nextValidOrderId + i])
            self.requestcounter +=1
            self.throttle()
            self.reqHistoricalData(self.nextValidOrderId + i, self.contracts[i], '20191121 16:11:00','3600 S',
                                   '5 secs', 'TRADES',0, 1, False,[])

app = App()
app.connect(host = "127.0.0.1", port = 7496, clientId=0)
app.run()


