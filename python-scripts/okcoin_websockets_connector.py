#!/usr/bin/python3
__author__ = "donnydevito"
__copyright__   = "Copyright 2015, donnydevito"
__license__ = "MIT"

import os, websocket, time, datetime, sys, json, hashlib, zlib, base64, json, re, elasticsearch, argparse, uuid, pytz, logging
from create_mappings import createMappings
from pytz import timezone
from datetime import timedelta
OKCOIN_WEBSOCKET_URL = "wss://real.okcoin.com:10440/websocket/okcoinapi"

# UTC ALL THE TIME, FOREVER AND EVER. 
TIMEZONE = pytz.timezone('UTC')

# ***** CHANGE THIS TO BE THE URL OF YOUR ELASTICSEARCH SERVER *****
ELASTICSEARCH_HOST = "http://localhost:9200"

# Default index name in elasticsearch to use for the btc_usd market data aggregation
DEFAULT_INDEX_NAME = "btcwebsockettickerarchive"

es = None

def getJsonData(okcoinData):
	tempData = okcoinData
	dataStr = tempData.decode(encoding='UTF-8')
	jsonData =json.loads(dataStr)
	return jsonData

def on_open(self):
    self.send("{'event':'addChannel','channel':'ok_btcusd_ticker','binary': 'true'}")
    self.send("{'event':'addChannel','channel':'ok_btcusd_depth', 'binary': 'true'}")
    self.send("{'event':'addChannel','channel':'ok_btcusd_trades_v1', 'binary': 'true'}")

    ## TODO
    # Request 
	# {'event':'addChannel','channel':'ok_btcusd_trades_v1'}
	# Response
	# [{
	#   "channel":"ok_btcusd_trades_v1",
	#   "data":[["1001","2463.86","0.052","16:34:07","ask"]]
	# }]
	  

def on_message(self, event):
	okcoinData = inflate(event) #data decompress
	jsonData = getJsonData(okcoinData)
	print("-----")
	for item in jsonData: 
		curChannel = item["channel"]
		if curChannel == "ok_btcusd_ticker": 
			injectTickerData(self, event, item)
		elif curChannel == "ok_btcusd_depth": 
			processOrderbook(self, event, item) 
		elif curChannel == "ok_btcusd_trades_v1": 
			printData(jsonData)
		else: 
			print("WTF")
	print("-----") 

	pass

def printData(jsonData):
	print(len(jsonData))

	for item in jsonData: 
		uniqueId = str(uuid.uuid4())
		if "data" in item: 
			curData = item["data"]
			for curOrder in curData: 
				completedTradeDto = {}
				asiaTimeZone = pytz.timezone("Asia/Shanghai")
				# os.environ["TZ"] = "Asia/Shanghai"
				# time.tzset()
				dateRecv = datetime.datetime.strptime(curOrder[3], "%H:%M:%S")
				theHour = dateRecv.hour
				theMinute = dateRecv.minute
				theSecond = dateRecv.second
				dateOccurred = dateRecv.replace(year=datetime.datetime.now(timezone("Asia/Shanghai")).year, month=datetime.datetime.now(timezone("Asia/Shanghai")).month, day=datetime.datetime.now(timezone("Asia/Shanghai")).day, hour=theHour, minute=theMinute, second=theSecond)
				fuckingit = timedelta(hours=8)

				# localDate = utcTz.localize(dateOccurred, is_dst=None).astimezone(pytz.utc)
				# [tid, price, amount, time, type]

				# fuckingtimebullshit = timedelta(hours=8)

				realDate = dateOccurred - fuckingit

				now_aware = pytz.utc.localize(realDate)


				theId = str(curOrder[0])
				thePrice = float(curOrder[1])
				theAmount = float(curOrder[2])
				theType = str(curOrder[4])
				completedTradeDto["uuid"] = uniqueId
				completedTradeDto["date"] = now_aware
				completedTradeDto["tradeId"] = theId
				completedTradeDto["timestamp"] = None
				completedTradeDto["amount"] = theAmount
				completedTradeDto["type"] = theType

				print (completedTradeDto)
				
				putNewDocumentRequest = es.create(index="btc_completed_trades", doc_type='ok_coin_completed_trade', ignore=[400], id=uuid.uuid4(), body=completedTradeDto)	
				successful = putNewDocumentRequest["created"]
				if successful == True: 
					print("OKCOIN COMPLETED ORDER DATA STORED.")
				else: 
					print("!! FATAL !!: WEBSOCKET ENTRY NOT ADDED TO ES CLUSTER")
				pass
				# realDateStr = datetime.datetime.strftime(realDate, "%Y-%m-%d %H:%M:%S")
				# print(realDateStr)
				# utcHour = int(str(dateRecv[0:2]))
				# print(utcHour)
				# print (asiaDateStr)
		# 			okCoinCompletedTradeMapping = { 
		# 	"ok_coin_completed_trade": {
		# 		"properties": { 
		# 			"uuid": { "type": "string", "index": "no" }, 
		# 			"date" : { "type": "date" }, 
		# 			"tradeId" : { "type" : "string", "index":"not_analyzed"}, 
		# 			"timestamp": {"type": "string", "index": "no"},
		# 			"price": {"type": "float"}, 
		# 			"amount": {"type": "float"},
		# 			"order_type" : { "type": "string"} 
		# 		}
		# 	}
		# }
def processOrderbook(self, event, okcoinData): 	
	if "data" in okcoinData: 
		orderData = okcoinData["data"]
		okCoinTimestamp = orderData["timestamp"]
		uniqueId = uuid.uuid4()
		dateRecv = datetime.datetime.fromtimestamp((float(okCoinTimestamp) / 1000), TIMEZONE)
		for order in orderData["bids"]: 
			okcoinOrderDto = {}
			okcoinOrderDto["uuid"] = str(uniqueId)
			okcoinOrderDto["date"] = dateRecv
			okcoinOrderDto["price"] = float(order[0])
			okcoinOrderDto["amount"] = float(order[1])
			okcoinOrderDto["order_type"] = "BID" 
			addOrderBookItem(self, event, okcoinOrderDto, "okcoin_order_book")

		for order in orderData["asks"]: 
			okcoinOrderDto = {}
			okcoinOrderDto["uuid"] = str(uniqueId)
			okcoinOrderDto["date"] = dateRecv
			okcoinOrderDto["price"] = float(order[0])
			okcoinOrderDto["amount"] = float(order[1] * -1)
			okcoinOrderDto["order_type"] = "ASK" 
			addOrderBookItem(self, event, okcoinOrderDto, "okcoin_order_book")

	pass


def addOrderBookItem(self, event, dto, doctype): 
	putNewDocumentRequest = es.create(index="btc_orderbooks", doc_type=doctype, ignore=[400], id=uuid.uuid4(), body=dto)
	successful = putNewDocumentRequest["created"]
	logging.info(successful)
	if successful == True: 
		print("WEBSOCKET ENTRY FOR " + doctype + " ADDED TO ES CLUSTER")
	else: 
		print("!! FATAL !!: WEBSOCKET ENTRY NOT ADDED TO ES CLUSTER")

def injectTickerData(self, event, data): 
	dataItem = data["data"]
	processTickerData(self, event, dataItem)
	uniqueIdentifier = uuid.uuid4()
	pass 

def processTickerData(self, event, item):
	okCoinDto = {}
	okCoinTimestamp = item["timestamp"]
	uniqueId = uuid.uuid4()
	dateRecv = datetime.datetime.fromtimestamp((float(okCoinTimestamp) / 1000), TIMEZONE)
	volume = item["vol"]
	volume = volume.replace(",", "") 
	lastPrice = item["last"]
	highPrice = item["high"]
	askPrice = item["sell"]
	lowPrice = item["low"]
	bidPrice = item["buy"]
	okCoinDto["uuid"] = str(uniqueId)
	okCoinDto["date"] = dateRecv
	okCoinDto["timestamp"] = str(okCoinTimestamp)
	okCoinDto["last_price"] = float(lastPrice)
	okCoinDto["volume"] = float(volume)
	okCoinDto["high"] = float(highPrice) 
	okCoinDto["ask"] = float(askPrice)
	okCoinDto["low"] = float(lowPrice)
	okCoinDto["bid"] = float(bidPrice)
	putNewDocumentRequest = es.create(index="btc_tickers", doc_type='okcoin_ticker', ignore=[400], id=uniqueId, body=okCoinDto)	
	successful = putNewDocumentRequest["created"]
	if successful == True: 
		print("WEBSOCKET ENTRY FOR DOCTYPE: okcoin_ticker ADDED TO ES CLUSTER")
	else: 
		print("!! FATAL !!: WEBSOCKET ENTRY NOT ADDED TO ES CLUSTER")
	pass

def inflate(okcoinData):
    decompressedData = zlib.decompressobj(
            -zlib.MAX_WBITS 
    )
    inflatedData = decompressedData.decompress(okcoinData)
    inflatedData += decompressedData.flush()
    return inflatedData

def on_error(self, event):
	print('EVENT IS: ') 
	print (event)

def on_close(self, event):
    print (event)

if __name__ == "__main__":
	es = elasticsearch.Elasticsearch([ELASTICSEARCH_HOST])
	createMappings(es, DEFAULT_INDEX_NAME)
	websocket.enableTrace(False)
	ws = websocket.WebSocketApp(OKCOIN_WEBSOCKET_URL, on_message = on_message, on_error = on_error, on_close = on_close)
	ws.on_open = on_open
	ws.run_forever()
