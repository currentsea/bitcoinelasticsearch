#!/usr/bin/python3
__author__ = "donnydevito"
__copyright__   = "Copyright 2015, donnydevito"
__license__ = "MIT"

import json, uuid, datetime, pytz, elasticsearch
from websocket import create_connection

# Default index name in elasticsearch to use for the btc_usd market data aggregation
DEFAULT_INDEX_NAME = "btcwebsockettickerarchive"

# UTC ALL THE TIME, FOREVER AND EVER. 
TIMEZONE = pytz.timezone('UTC')

# ***** CHANGE THIS TO BE THE URL OF YOUR ELASTICSEARCH SERVER *****
ELASTICSEARCH_HOST = "http://localhost:9200"

## TODO: Reduce redundancy here for mappings
def createMappings(es): 
	mappingCreated = False
	try: 
		bitfinexMapping = {
			"bitfinex": {
				"properties": {
					"uuid": { "type": "string", "index": "no"}, 
					"date": {"type": "date"},
					"last_price": {"type": "float"},
					"timestamp": {"type": "string", "index": "no"},
					"volume": {"type": "float"},
					"high": {"type": "float"},
					"ask": {"type": "float"},
					"low": {"type": "float"},
					"dailyChange": {"type": "float"}, 
					"dailyDelta": {"type" : "float"}, 
					"askVolume": {"type": "float"}, 
					"bidVolume": {"type": "float"},
					"bid": {"type": "float"}
				}
			}
		}
		okcoinMapping = { 
			"okcoin": { 
				"properties": {
					"uuid": { "type": "string", "index": "no"}, 
					"date": {"type":"date"}, 
					"last_price": {"type": "float"}, 
					"timestamp": {"type": "string", "index": "no"},
					"volume": {"type": "float"},
					"high": {"type": "float"},
					"ask": {"type": "float"},
					"low": {"type": "float"},
					"bid": {"type": "float"}
				}
			}
		}
		bitfinexOrderBookMapping = { 
			"bitfinex_order_book": { 
				"properties": { 
					"uuid": { "type": "string", "index": "no"}, 
					"date": {"type":"date"}, 
					"orders" : { 
		                "type" : "nested",
		                "properties": { 
							"price": { "type": "float"},
							"amount": {"type": "float"}, 
							"count": {"type": "float"}, 
							"order_type": {"type": "string"} 
		                }
					}
					# "largest_bid_order_weighted_by_volume"
					# "largest_ask_order_weighted_by_volume"
					# "largest_order_by_volume" 
					# "standard_deviation_orders"
					# "new_order_delta"

				}

			}
		} 

		okcoinOrderBookMapping = { 
			"okcoin_order_book": { 
				"properties": { 
					"uuid": { "type": "string", "index": "no"}, 
					"date": {"type":"date"}, 
					"orders" : { 
		                "type" : "nested",
		                "properties": { 
							"price": { "type": "float"},
							"amount": {"type": "float"}, 
							"order_type" : { "type": "string"} 
		                }
					}
					# "largest_bid_order_weighted_by_volume"
					# "largest_ask_order_weighted_by_volume"
					# "largest_order_by_volume" 
					# "standard_deviation_orders"
					# "new_order_delta"
				}

			}
		} 
		bitfinexMarginBookMapping = "bitfinex_marginbook": { 
			"uuid":{"type": "float"}
		    "date":{"type": "date"},
		    "rate":{ "type": "float"},
		    "amount":{"type": "float"},
		    "period":{"type": "float"},
		    "timestamp": {"type": "float", "index": "no"},
		    "frr":{"type": "string"}
		}
		es.indices.create(DEFAULT_INDEX_NAME)
		es.indices.put_mapping(index=DEFAULT_INDEX_NAME, doc_type="bitfinex", body=bitfinexMapping)
		es.indices.put_mapping(index=DEFAULT_INDEX_NAME, doc_type="okcoin", body=okcoinMapping)
		es.indices.put_mapping(index=DEFAULT_INDEX_NAME, doc_type="bitfinex_order_book", body=bitfinexOrderBookMapping)
		es.indices.put_mapping(index=DEFAULT_INDEX_NAME, doc_type="okcoin_order_book", body=okcoinOrderBookMapping)
		es.indices.put_mapping(index=DEFAULT_INDEX_NAME, doc_type="bitfinex_marginbook", body=bitfinexMarginBookMapping)

		mappingCreated = True
	except: 
		pass
	return mappingCreated


def injectOrderBlock(orderbook, es, recordDate, uniqueId): 
	for item in orderbook: 
		orderDto = {}
		thePrice = item[0]
		orderDto["uuid"] = uniqueId
		orderDto["date"] = recordDate
		orderDto["price"] = float(thePrice)
		theCount = item[1]
		orderDto["count"] = float(theCount)
		theAmount = item[2] 

		theAmount = float(theAmount) 

		if theAmount < 0: 
			orderDto["order_type"] = "ASK"
		else: 
			orderDto["order_type"] = "BID" 
			
		orderDto["amount"] = float(theAmount)



		putNewDocumentRequest = es.create(index=DEFAULT_INDEX_NAME, doc_type='bitfinex_order_book', ignore=[400], id=uuid.uuid4(), body=orderDto)
		successful = putNewDocumentRequest["created"]
		if successful == True: 
			print("ES Entry Added: " + uniqueId)  
		else: 
			print("ES Entry failed to POST: " + uniqueId)  


def run(): 
	es = elasticsearch.Elasticsearch([ELASTICSEARCH_HOST])
	createMappings(es)
	ws = create_connection("wss://api2.bitfinex.com:3000/ws")

	ws.send(json.dumps({
	    "event": "subscribe",
	    "channel": "ticker",
	    "pair": "BTCUSD"
	}))

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
	    "pair": "BTCUSD",
	    "prec": "P0",
	    "len":"100"	
	}))

	bookChannel = None
	tickerChannel = None
	marginChannel = None

	while (bookChannel == None or tickerChannel == None or marginChannel == None):
		result = ws.recv()
		result = json.loads(result)

		# Channel the FORCE
		if "channel" in result: 
			print("hi")
			channel = result["channel"]
			if channel == "book": 
				bookChannel = result["chanId"]
				print("BOOK CHANNEL " + str(bookChannel))
			elif channel == "ticker": 
				tickerChannel = result["chanId"]
				print("TICKER CHANNEL " + str(tickerChannel))
			else: 
				print("These aren't the droids you're looking for.")

	while True:
		recordDate = datetime.datetime.now(TIMEZONE)
		uniqueId = str(uuid.uuid4())
		result = ws.recv()
		result = json.loads(result)
		curChannel = result[0]
		if curChannel == bookChannel: 
			if len(result) == 2: 
				if (result[1] == 'hb'): 
					print("HEARTBEAT!") 
				else: 
					print("Injecting Initial Orderbook on WS Connect... ID: " + uniqueId) 
					injectOrderBlock(result[1], es, recordDate, uniqueId)
			elif len(result) == 4: 
				singleOrdeEntry = [result[1:]]
				injectOrderBlock(singleOrdeEntry, es, recordDate, uniqueId) 
		elif curChannel == tickerChannel: 
			if (len(result) == 11): 
				bidPrice = float(result[1])
				bidVol = float(result[2]) 
				askPrice = float(result[3]) 
				askVol = float(result[4]) 
				dailyChange = float(result[5]) 
				dailyDelta = float(result[6]) 
				lastPrice = float(result[7]) 
				volume = float(result[8]) 
				highPrice = float(result[9])
				lowPrice = float(result[10])
				bitfinexTickerDict = {}
				bitfinexTickerDict["uuid"] = uniqueId
				bitfinexTickerDict["date"] = recordDate
				bitfinexTickerDict["last_price"] = lastPrice
				bitfinexTickerDict["volume"] = volume 
				bitfinexTickerDict["high"] = highPrice
				bitfinexTickerDict["ask"] = askPrice
				bitfinexTickerDict["low"] = lowPrice
				bitfinexTickerDict["bid"] = bidPrice
				bitfinexTickerDict["dailyChange"] = dailyChange
				bitfinexTickerDict["dailyDelta"] = dailyDelta
				bitfinexTickerDict["askVolume"] = askVol
				bitfinexTickerDict["bidVolume"] = bidVol
				putNewDocumentRequest = es.create(index=DEFAULT_INDEX_NAME, doc_type='bitfinex', ignore=[400], id=uniqueId, body=bitfinexTickerDict)
				successful = putNewDocumentRequest["created"]
				if successful == True: 
					print("Added ticker data to ES cluster: " + uniqueId) 
				else: 
					print("!! FATAL !!: WEBSOCKET ENTRY NOT ADDED TO ES CLUSTER")
	ws.close()


if __name__ == "__main__": 
	run()
	