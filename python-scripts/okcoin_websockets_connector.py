#!/usr/bin/python3
__author__ = "donnydevito"
__copyright__   = "Copyright 2015, donnydevito"
__license__ = "MIT"

import websocket, time, datetime, sys, json, hashlib, zlib, base64, json, re, elasticsearch, argparse, uuid, pytz
from create_mappings import createMappings

BITFINEX_WEBSOCKET_URL = "wss://api2.bitfinex.com:3000/ws"
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
	print(jsonData)
	return jsonData

def on_open(self):
    self.send("{'event':'addChannel','channel':'ok_btcusd_ticker','binary':'true'}")
    self.send("{'event':'addChannel','channel':'ok_btcusd_depth'}")
    # self.send("{'event': 'addChannel', 'channel': 'ok_btcusd_future_ticker_this_week'}")
  
def on_message(self, event):
	okcoinData = inflate(event) #data decompress
	jsonData = getJsonData(okcoinData)
	curChannel = jsonData[0]["channel"]
	if curChannel == "ok_btcusd_ticker": 
		injectTickerData(okcoinData)
	elif curChannel == "ok_btcusd_depth": 
		processOrderbook(okcoinData) 
	# elif curChannel == "ok_btcusd_future_ticker_this_week":
	# 	processThisWeekFutureTicker(okcoinData)


def processThisWeekFutureTicker(okcoinData): 
	jsonData = getJsonData(okcoinData)
	futureDto = {}
	for jsonItem in jsonData: 
		uniqueId = uuid.uuid4()
		if "data" in jsonItem: 
			recordDate = datetime.datetime.now(TIMEZONE)
			orderData = jsonItem["data"]
			futureDto["uuid"] = str(uniqueId) 
			futureDto["date"] = recordDate
			futureDto["contractId"] = str(orderData["contractId"])
			futureDto["high"] = float(orderData["high"]) 
			futureDto["buy"] = float(orderData["buy"])
			futureDto["low"] = float(orderData["low"]) 
			futureDto["last"] = float(orderData["last"]) 
			futureDto["sell"] = float(orderData["sell"]) 
			futureDto["amount"] = float(orderData["unitAmount"]) 
			futureDto["volume"] = float(orderData["volume"]) 
			addOrderBookItem(futureDto, "ok_coin_futures_this_week")


def processOrderbook(okcoinData): 
	jsonData = getJsonData(okcoinData) 
	
	for jsonItem in jsonData: 
		uniqueId = uuid.uuid4()
		if "data" in jsonItem: 
			orderData = jsonItem["data"]
			okCoinTimestamp = orderData["timestamp"]
			dateRecv = datetime.datetime.fromtimestamp((float(okCoinTimestamp) / 1000), TIMEZONE)
			
			for order in orderData["bids"]: 
				okcoinOrderDto = {}
				okcoinOrderDto["uuid"] = str(uniqueId)
				okcoinOrderDto["date"] = dateRecv
				okcoinOrderDto["price"] = float(order[0])
				okcoinOrderDto["amount"] = float(order[1])
				okcoinOrderDto["order_type"] = "BID" 
				addOrderBookItem(okcoinOrderDto, "okcoin_order_book")

			for order in orderData["asks"]: 
				okcoinOrderDto = {}
				okcoinOrderDto["uuid"] = str(uniqueId)
				okcoinOrderDto["date"] = dateRecv
				okcoinOrderDto["price"] = float(order[0])
				okcoinOrderDto["amount"] = float(order[1] * -1)
				okcoinOrderDto["order_type"] = "ASK" 
				addOrderBookItem(okcoinOrderDto, "okcoin_order_book")

	pass


def addOrderBookItem(dto, doctype): 
	putNewDocumentRequest = es.create(index=DEFAULT_INDEX_NAME, doc_type=doctype, ignore=[400], id=uuid.uuid4(), body=dto)
	successful = putNewDocumentRequest["created"]
	if successful == True: 
		print("WEBSOCKET ENTRY ADDED TO ES CLUSTER " + doctype)
	else: 
		print("!! FATAL !!: WEBSOCKET ENTRY NOT ADDED TO ES CLUSTER")

def injectTickerData(data): 
	tempData = data
	dataStr = tempData.decode(encoding='UTF-8')
	jsonData =json.loads(dataStr)
	for item in jsonData: 
		dataItem = item["data"]
		processTickerData(dataItem)
	uniqueIdentifier = uuid.uuid4()
	return data

def processTickerData(item): 
	itemDict = dict(item)
	okCoinDto = {}
	okCoinTimestamp = itemDict["timestamp"]
	uniqueId = uuid.uuid4()
	dateRecv = datetime.datetime.fromtimestamp((float(okCoinTimestamp) / 1000), TIMEZONE)
	volume = itemDict["vol"]
	volume = volume.replace(",", "") 
	lastPrice = itemDict["last"]
	highPrice = itemDict["high"]
	askPrice = itemDict["sell"]
	lowPrice = itemDict["low"]
	bidPrice = itemDict["buy"]
	okCoinDto["uuid"] = str(uniqueId)
	okCoinDto["date"] = dateRecv
	okCoinDto["timestamp"] = str(okCoinTimestamp)
	okCoinDto["last_price"] = float(lastPrice)
	okCoinDto["volume"] = float(volume) 
	okCoinDto["high"] = float(highPrice) 
	okCoinDto["ask"] = float(askPrice)
	okCoinDto["low"] = float(lowPrice)
	okCoinDto["bid"] = float(bidPrice)
	putNewDocumentRequest = es.create(index=DEFAULT_INDEX_NAME, doc_type='okcoin_ticker', ignore=[400], id=uniqueId, body=okCoinDto)
	successful = putNewDocumentRequest["created"]
	if successful == True: 
		print("WEBSOCKET ENTRY ADDED TO ES CLUSTER")
	else: 
		print("!! FATAL !!: WEBSOCKET ENTRY NOT ADDED TO ES CLUSTER")
	return item

def inflate(okcoinData):
    decompressedData = zlib.decompressobj(
            -zlib.MAX_WBITS 
    )
    inflatedData = decompressedData.decompress(okcoinData)
    inflatedData += decompressedData.flush()
    return inflatedData

def on_error(self, event):
    print("ERROR IS")
    print(event)

def on_close(self, event):
    print(event)

if __name__ == "__main__":
	es = elasticsearch.Elasticsearch([ELASTICSEARCH_HOST])
	mappings = createMappings(es, DEFAULT_INDEX_NAME)
	print("CREATED MAPPINGS: " + str(mappings))
	websocket.enableTrace(False)
	ws = websocket.WebSocketApp(OKCOIN_WEBSOCKET_URL, on_message = on_message, on_error = on_error, on_close = on_close)
	ws.on_open = on_open
	ws.run_forever()

