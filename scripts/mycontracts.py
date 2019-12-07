from ibapi.contract import Contract

def createContractObject(tickerlist):
    obj = [Contract() for i in range(len(tickerlist))]
    for cont,ticker in zip(obj,tickerlist):
        cont.symbol = ticker
        cont.secType = "STK"
        cont.currency = "AUD"
        cont.exchange = "ASX"

    return obj




