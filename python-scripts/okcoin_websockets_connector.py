#!/usr/bin/python3
__author__ = "donnydevito"
__copyright__   = "Copyright 2015, donnydevito"
__license__ = "MIT"

import os, websocket, time, datetime, sys, json, hashlib, zlib, base64, json, re, elasticsearch, argparse, uuid, pytz
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

CANDLE_LIST = [ "ok_btcusd_kline_1min", "ok_btcusd_kline_3min", "ok_btcusd_kline_5min", "ok_btcusd_kline_15min", "ok_btcusd_kline_30min", "ok_btcusd_kline_1hour", "ok_btcusd_kline_2hour", "ok_btcusd_kline_4hour", "ok_btcusd_kline_6hour", "ok_btcusd_kline_12hour", "ok_btcusd_kline_day", "ok_btcusd_kline_3day", "ok_btcusd_kline_week" ] 

FUTURES_CONTRACT_TYPES = ["ok_btcusd_future_ticker_this_week", "ok_btcusd_future_ticker_next_week", "ok_btcusd_future_ticker_quarter"]

es = None

def getArgs(): 
	parser = argparse.ArgumentParser(description='BTC elastic search data collector')
	parser.add_argument('--host')
	parser.add_argument('--forever', action='store_true', default=False)
	parser.add_argument('--max_records', action='store_true', default=3600)

	# TODO: add more params here

	args = parser.parse_args()
	return args

def getJsonData(okcoinData):
	tempData = okcoinData
	dataStr = tempData.decode(encoding='UTF-8')
	jsonData =json.loads(dataStr)
	return jsonData

def on_open(self):
	self.send("{'event':'addChannel','channel':'ok_btcusd_ticker','binary': 'true'}")
	self.send("{'event':'addChannel','channel':'ok_btcusd_depth', 'binary': 'true'}")
	self.send("{'event':'addChannel','channel':'ok_btcusd_trades_v1', 'binary': 'true'}")
	self.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_1min', 'binary':'true'}")
	self.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_3min', 'binary':'true'}")
	self.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_5min', 'binary':'true'}")
	self.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_15min', 'binary':'true'}")
	self.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_30min', 'binary':'true'}")
	self.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_1hour', 'binary':'true'}")
	self.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_2hour', 'binary':'true'}")
	self.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_4hour', 'binary':'true'}")
	self.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_6hour', 'binary':'true'}")
	self.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_12hour', 'binary':'true'}")
	self.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_day', 'binary':'true'}")
	self.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_3day', 'binary':'true'}")
	self.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_week', 'binary':'true'}")
	self.send("{'event':'addChannel','channel':'ok_btcusd_future_ticker_this_week', 'binary': 'true'}")
	self.send("{'event':'addChannel','channel':'ok_btcusd_future_ticker_next_week', 'binary': 'true'}")
	self.send("{'event':'addChannel','channel':'ok_btcusd_future_ticker_quarter', 'binary': 'true'}")
	self.send("{'event':'addChannel','channel':'ok_btcusd_future_index', 'binary':'true'}")

def on_message(self, event):
	okcoinData = inflate(event) #data decompress
	jsonData = getJsonData(okcoinData)
	for item in jsonData: 
		curChannel = item["channel"]
		if curChannel == "ok_btcusd_ticker": 
			injectTickerData(self, event, item)
		elif curChannel == "ok_btcusd_depth": 
			processOrderbook(self, event, item) 
		elif curChannel == "ok_btcusd_trades_v1": 
			processCompletedTrades(jsonData)
		elif curChannel in CANDLE_LIST: 
			processCandleStick(curChannel, item)
		elif curChannel in FUTURES_CONTRACT_TYPES: 
			processTheFuture(curChannel, item) 
		elif curChannel == "ok_btcusd_future_index": 
			indexTheFuture(curChannel, item)
		else: 
			print("WTF")
	print("-----") 
	pass

def indexTheFuture(futureChannel, futureData):
	data = futureData["data"] 
	uniqueId = uuid.uuid4()
	futureIndexDto = {}
	# futureIndex: current futures index price
	# timestamp: 
	theDate = datetime.datetime.fromtimestamp(float(data["timestamp"]) / 1000, TIMEZONE)
	thePrice = float(data["futureIndex"])
	futureIndexDto = {}
	futureIndexDto["uuid"] = str(uniqueId)
	futureIndexDto["date"] = theDate
	futureIndexDto["price"] = thePrice
	futureIndexDto["timestamp"] = str(data["timestamp"])
	createTheFuture("btc_futures", "ok_btcusd_future_index", futureIndexDto)

def processTheFuture(futureType, jsonData): 
	futureData = jsonData["data"] 

	futureRegex = re.search("ok_btcusd_future_ticker_(.+)", futureType)
	futureType = futureRegex.group(1)
	# {'vol': '696068.00', 'high': 471.64, 'contractId': '20160325012', 'low': 461.74, 'buy': 464.68, 'last': '464.68', 'hold_amount': 235764, 'unitAmount': 100, 'sell': 464.76}
	futureDto = {}
	uniqueId = uuid.uuid4()
	recordDate = datetime.datetime.now(TIMEZONE)
	futureDto["uuid"] = str(uniqueId) 
	futureDto["date"] = recordDate
	futureDto["volume"] = float(futureData["vol"])
	futureDto["high"] = float(futureData["high"]) 
	futureDto["contract_id"] = str(futureData["contractId"]) 
	futureDto["low"] = float(futureData["low"]) 
	futureDto["buy"] = float(futureData["buy"]) 
	futureDto["last"] = float(futureData["last"]) 
	futureDto["hold_amount"] = float(futureData["hold_amount"]) 
	futureDto["unit_amount"] = float(futureData["unitAmount"]) 
	futureDto["sell"] = float(futureData["sell"]) 
	futureDto["contract_type"] = str(futureType)
	createTheFuture("btc_futures", "ok_btcusd_future_ticker", futureDto)

def createTheFuture(index, doctype, data): 
	putNewDocumentRequest = es.create(index=index, doc_type=doctype, ignore=[400], id=uuid.uuid4(), body=data)
	successful = putNewDocumentRequest["created"]
	if successful == True: 
		print("WEBSOCKET ENTRY FOR " + doctype + " ADDED TO ES CLUSTER")
	else: 
		print("!! FATAL !!: WEBSOCKET ENTRY NOT ADDED TO ES CLUSTER")

def processCandleStick(candleType, jsonData): 
	dataObj = jsonData["data"]
	dataPoint = dataObj
	if len(dataPoint) == 6: 
		print(dataPoint)
		candleDto = {}
		uniqueId = uuid.uuid4()
		okCoinTimestamp = dataPoint[0]
		candleDto["uuid"] = str(uniqueId) 
		candleDto["date"] = datetime.datetime.utcnow()
		candleDto["candle_type"] = candleType
		candleDto["timestamp"] = str(okCoinTimestamp) 
		candleDto["open_price"] = float(dataPoint[1])
		candleDto["highest_price"] = float(dataPoint[2])
		candleDto["lowest_price"] = float(dataPoint[3])
		candleDto["close_price"] = float(dataPoint[4])
		volVal = str(dataPoint[5])
		volVal = volVal.replace(",", "")
		candleDto["volume"] = float(volVal)
		putNewDocumentRequest = es.create(index="btc_candlesticks", doc_type='ok_coin_candlestick', ignore=[400], id=str(uuid.uuid4()), body=candleDto)	
		successful = putNewDocumentRequest["created"]
		if successful == True: 
			print("OKCOIN CANDLESTICK DATA STORED.")
		else: 
			print("!! FATAL !!: WEBSOCKET ENTRY NOT ADDED TO ES CLUSTER")
		pass

def processCompletedTrades(jsonData):
	print(len(jsonData))

	for item in jsonData: 
		uniqueId = str(uuid.uuid4())
		if "data" in item: 
			curData = item["data"]
			for curOrder in curData: 
				if type(curOrder) is list: 
					completedTradeDto = {}
					theId = str(curOrder[0])
					thePrice = float(curOrder[1])
					theAmount = float(curOrder[2])
					theType = str(curOrder[4])

					theType = theType.upper() 
					if theType == "ASK": 
						theAmount = theAmount * -1

					completedTradeDto["uuid"] = uniqueId
					completedTradeDto["date"] = datetime.datetime.utcnow()
					completedTradeDto["price"] = thePrice
					completedTradeDto["tradeId"] = theId
					completedTradeDto["timestamp"] = str(curOrder[3])
					completedTradeDto["amount"] = theAmount
					completedTradeDto["order_type"] = theType				
					putNewDocumentRequest = es.create(index="btc_completed_trades", doc_type='ok_coin_completed_trade', ignore=[400], id=uuid.uuid4(), body=completedTradeDto)	
					successful = putNewDocumentRequest["created"]
					if successful == True: 
						print("OKCOIN COMPLETED ORDER DATA STORED.")
					else: 
						print("!! FATAL !!: WEBSOCKET ENTRY NOT ADDED TO ES CLUSTER")
					pass

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

			# Amount is appended to be negative to keep conformity with bitfinex data 
			okcoinOrderDto["amount"] = float(order[1] * -1)
			okcoinOrderDto["order_type"] = "ASK" 
			addOrderBookItem(self, event, okcoinOrderDto, "okcoin_order_book")

	pass


def addOrderBookItem(self, event, dto, doctype): 
	putNewDocumentRequest = es.create(index="btc_orderbooks_live", doc_type=doctype, ignore=[400], id=uuid.uuid4(), body=dto)
	successful = putNewDocumentRequest["created"]
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
	print('ERROR IS: ') 
	print (event)

def on_close(self, event):
    print (event)

if __name__ == "__main__":
	args = getArgs() 
	if (args.host): 
		hostStrip = args.host
		hostStrip = hostStrip.strip()
		ELASTICSEARCH_HOST = hostStrip
	es = elasticsearch.Elasticsearch([ELASTICSEARCH_HOST])
	createMappings(es, DEFAULT_INDEX_NAME)
	websocket.enableTrace(False)
	ws = websocket.WebSocketApp(OKCOIN_WEBSOCKET_URL, on_message = on_message, on_error = on_error, on_close = on_close)
	ws.on_open = on_open
	ws.run_forever()
