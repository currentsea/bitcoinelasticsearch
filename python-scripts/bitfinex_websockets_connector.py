#!/usr/bin/python3
__author__ = "donnydevito"
__copyright__   = "Copyright 2015, donnydevito"
__license__ = "MIT"

import json, uuid, datetime, pytz, elasticsearch, argparse
from websocket import create_connection
from create_mappings import createMappings

# Default index name in elasticsearch to use for the btc_usd market data aggregation
DEFAULT_INDEX_NAME = "btcwebsockettickerarchive"

# UTC ALL THE TIME, FOREVER AND EVER. 
TIMEZONE = pytz.timezone('UTC')

# ***** CHANGE THIS TO BE THE URL OF YOUR ELASTICSEARCH SERVER *****
ELASTICSEARCH_HOST = "http://localhost:9200"

BITFINEX_WEBSOCKETS_URL = "wss://api2.bitfinex.com:3000/ws"

def getArgs(): 
	parser = argparse.ArgumentParser(description='BTC elastic search data collector')
	parser.add_argument('--host')
	parser.add_argument('--forever', action='store_true', default=False)
	parser.add_argument('--max_records', action='store_true', default=3600)

	# TODO: add more params here

	args = parser.parse_args()
	return args


def getCompletedTradeDto(completedTrade, uniqueId, recordDate): 
	tradeDto = {}

	tradeDto["uuid"] = uniqueId
	tradeDto["date"] = recordDate
	if len(completedTrade) == 4: 
		tradeDto["tradeId"] = str(completedTrade[0])
		tradeDto["timestamp"] = str(completedTrade[1])
		tradeDto["price"] = float(completedTrade[2]) 
		theAmount = float(completedTrade[3])
		tradeDto["amount"] = theAmount
		
	elif len(completedTrade) == 5: 
		tradeDto["tradeId"] = str(completedTrade[1])
		tradeDto["timestamp"] = str(completedTrade[2])
		tradeDto["price"] = float(completedTrade[3]) 
		theAmount = float(completedTrade[4]) 
		tradeDto["amount"] = theAmount

	elif len(completedTrade) == 6: 
		tradeDto["sequenceId"] = str(completedTrade[1])
		tradeDto["tradeId"] = str(completedTrade[2])
		tradeDto["timestamp"] = str(completedTrade[3])
		tradeDto["price"] = float(completedTrade[4]) 
		theAmount = float(completedTrade[5]) 

	if theAmount < 0: 
		tradeDto["order_type"] = "ASK"
	else: 
		tradeDto["order_type"] = "BID"
	return tradeDto

def getTickerDto(tickerData, uniqueId, recordDate): 
	bidPrice = float(tickerData[1])
	bidVol = float(tickerData[2]) 
	askPrice = float(tickerData[3]) 
	askVol = float(tickerData[4]) 
	dailyChange = float(tickerData[5]) 
	dailyDelta = float(tickerData[6]) 
	lastPrice = float(tickerData[7]) 
	volume = float(tickerData[8]) 
	highPrice = float(tickerData[9])
	lowPrice = float(tickerData[10])
	bitfinexTickerDto = {}
	bitfinexTickerDto["uuid"] = uniqueId
	bitfinexTickerDto["date"] = recordDate
	bitfinexTickerDto["last_price"] = lastPrice
	bitfinexTickerDto["volume"] = volume 
	bitfinexTickerDto["high"] = highPrice
	bitfinexTickerDto["ask"] = askPrice
	bitfinexTickerDto["low"] = lowPrice
	bitfinexTickerDto["bid"] = bidPrice
	bitfinexTickerDto["dailyChange"] = dailyChange
	bitfinexTickerDto["dailyDelta"] = dailyDelta
	bitfinexTickerDto["askVolume"] = askVol
	bitfinexTickerDto["bidVolume"] = bidVol
	return bitfinexTickerDto

def getOrderBookDto(orderBookData, uniqueId, recordDate): 
	orderDto = {}
	offSet = 0
	if (len(orderBookData) == 4): 
		offSet = 1
	elif (len(orderBookData) == 3): 
		offSet = 0
	else: 
		print ("pass --")
		pass

	thePrice = orderBookData[0 + offSet]
	orderDto["uuid"] = uniqueId
	orderDto["date"] = recordDate
	orderDto["price"] = float(thePrice)
	theCount = orderBookData[1 + offSet]
	orderDto["count"] = float(theCount)
	theAmount = orderBookData[2 + offSet] 
	theAmount = float(theAmount) 
	if theAmount < 0: 
		orderDto["order_type"] = "ASK"
	else: 
		orderDto["order_type"] = "BID" 
	orderDto["amount"] = float(theAmount)
	return orderDto 	

def injectOrderBook(es, orderbook, uniqueId, recordDate, indexName="btc_orderbooks_live", docType="bitfinex_order_book"): 
	for item in orderbook: 
		orderDto = getOrderBookDto(item, uniqueId, recordDate)
		putNewDocumentRequest = es.create(index=indexName, doc_type=docType, ignore=[400], id=uuid.uuid4(), body=orderDto)
		successful = putNewDocumentRequest["created"]
		if successful == True: 
			print("[INDEX: " + indexName + "] [DOCTYPE: " + docType + "] Updated: " + uniqueId)  
		else: 
			print("ES Entry failed to POST: " + uniqueId)  

		# tickerIndex = "btc_tickers"
		# orderBookIndex = "btc_orderbooks"
		# completedTradesIndex = "btc_completed_trades"
def injectCompletedTrade(es, completedTrade, indexName="btc_completed_trades", docType="bitfinex_completed_trade"):
	putNewDocumentRequest = es.create(index=indexName, doc_type=docType, ignore=[400], id=uuid.uuid4(), body=completedTrade)
	successful = putNewDocumentRequest["created"]
	if successful == True: 
		print("[INDEX: " + indexName + "] [DOCTYPE: " + str(docType) + "] Updated: " + str(completedTrade["uuid"])) 
	else: 
		print("!! FATAL !!: WEBSOCKET ENTRY NOT ADDED TO ES CLUSTER")
	return successful 

def injectTickerData(es, tickerData, indexName="btc_tickers", docType="bitfinex_ticker"): 
	putNewDocumentRequest = es.create(index=indexName, doc_type=docType, ignore=[400], id=uuid.uuid4(), body=tickerData)
	successful = putNewDocumentRequest["created"]
	if successful == True: 
		print("[INDEX: " + indexName + "] [DOCTYPE: " + docType + "]: Updated" + str(tickerData["uuid"]))
	else: 
		print("!! FATAL !!: WEBSOCKET ENTRY for doc_tye: " + docType + " NOT ADDED TO ES CLUSTER")
	return successful

def run(): 
	es = elasticsearch.Elasticsearch([ELASTICSEARCH_HOST])
	mappings = createMappings(es, DEFAULT_INDEX_NAME) 
	print("MAPPINGS CREATED: " + str(mappings))
	ws = create_connection(BITFINEX_WEBSOCKETS_URL)
	# ws.send(json.dumps({
	#     "event": "subscribe",
	#     "channel": "ticker",
	#     "pair": "BTCUSD"
	# }))

	ws.send(json.dumps({
		"event": "subscribe",
	    "channel": "book",
	    "pair": "BTCUSD",
	    "prec": "P0",
	    "len":"100"	
	}))

	ws.send(json.dumps({
		"event": "subscribe",
	    "channel": "book",
	    "pair": "LTCUSD",
	    "prec": "P0",
	    "len":"100"	
	}))

	ws.send(json.dumps({
		"event": "subscribe",
	    "channel": "book",
	    "pair": "LTCBTC",
	    "prec": "P0",
	    "len":"100"	
	}))
	#["btcusd","ltcusd","ltcbtc","ethusd","ethbtc"]

	ws.send(json.dumps({
		"event": "subscribe",
	    "channel": "book",
	    "pair": "ETHUSD",
	    "prec": "P0",
	    "len":"100"	
	}))

	ws.send(json.dumps({
		"event": "subscribe",
	    "channel": "book",
	    "pair": "ethbtc",
	    "prec": "P0",
	    "len":"100"	
	}))
	# ws.send(json.dumps({ 
	#     "event": "subscribe",
	#     "channel": "trades",
	#     "pair": "BTCUSD"
	# }))

	# bookChannel = None
	# tickerChannel = None
	# tradeChannel = None

	# while (tickerChannel == None or bookChannel == None or tradeChannel == None):
	# 	result = ws.recv()
	# 	result = json.loads(result)
	# 	# Channel the FORCE
	# 	if "channel" in result: 
	# 		channel = result["channel"]
	# 		if channel == "book": 
	# 			bookChannel = result["chanId"]
	# 			print("BOOK CHANNEL " + str(bookChannel))
	# 		elif channel == "ticker": 
	# 			tickerChannel = result["chanId"]
	# 			print("TICKER CHANNEL " + str(tickerChannel))
	# 		elif channel == "trades": 
	# 			tradeChannel = result["chanId"] 
	# 			print("TRADES CHANNEL: " + str(tradeChannel))
	# 		else: 
	# 			print("These aren't the droids you're looking for.")

	while True:
		recordDate = datetime.datetime.now(TIMEZONE)
		uniqueId = str(uuid.uuid4())
		result = ws.recv()
		try: 
			result = json.loads(result) 
			print (result)
		except:
			result = ""
			pass

		# try: 
		# 	result = json.loads(result)
		# 	curChannel = result[0]
		# except: 
		# 	result = ""

		# if curChannel == bookChannel: 
		# 	if len(result) == 2: 
		# 		if (result[1] == 'hb'): 
		# 			#print("ORDER BOOK HEARTBEAT!") 
		# 			pass
		# 		else: 
		# 			print("Injecting Initial Orderbook on WS Connect... ID: " + uniqueId) 
		# 			injectOrderBook(es, result[1], uniqueId, recordDate)
		# 	elif len(result) == 4: 
		# 		injectOrderBook(es, [result], uniqueId, recordDate) 
		# 	else: 
		# 		print("BOOK CHANNEL DATA INVALID: (shown below)")
		# 		print(result) 

		# elif curChannel == tickerChannel: 
		# 	if len(result) == 11: 
		# 		tickerDto = getTickerDto(result, uniqueId, recordDate)
		# 		injectTickerData(es, tickerDto)
		# 	elif len(result) == 2: 
		# 		if result[1] == 'hb': 
		# 			#print("TICKER HEARTBEAT!")
		# 			pass
		# 		else: 
		# 			print("AWKWARD DATA (heartbeat but not heartbeat): ")
		# 			print(result[1])
		# 	else: 
		# 		print("MISSING DATAPOINT: ")
		# 		print(result) 

		# elif curChannel == tradeChannel: 
		# 	processTradeChannelData(es, result, uniqueId, recordDate)

		# else: 
		# 	print("DATA RECEIVED NOT RELEVANT TO ANY SUBSCRIBED CHANNELS") 

	ws.close()

def processTradeChannelData(es, result, uniqueId, recordDate): 
	if (result[1] == 'hb'): 
		#print("TRADES HEARTBEAT!") 
		pass
	else: 
		if type(result[1]) is list: 
			theData = result[1]
			for item in theData: 
				print ("ADDING BULK LIST OF BITFINEX COMPLETED TRADES: " + str(uniqueId)) 
				tradeDto = getCompletedTradeDto(item, uniqueId, recordDate)
				injectCompletedTrade(es, tradeDto)
		else: 
			theData = result
			dataLength = len(theData[1:])
			if dataLength == 5 or dataLength == 6: 
				tradeDto = getCompletedTradeDto(theData[1:], uniqueId, recordDate) 
				injectCompletedTrade(es, tradeDto)
			else: 
				print("!!! fatal !!!")
				print(theData)
				pass 
				
if __name__ == "__main__": 
	args = getArgs()
	if (args.host): 
		ELASTICSEARCH_HOST = args.host
	run()
	