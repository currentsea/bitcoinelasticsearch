#!/usr/bin/python3
__author__ = "donnydevito"
__copyright__   = "Copyright 2015, donnydevito"
__license__ = "MIT"

import websocket, time, datetime, sys, json, hashlib, zlib, base64, json, re, elasticsearch, argparse, uuid, pytz

BITFINEX_WEBSOCKET_URL = "wss://api2.bitfinex.com:3000/ws"
OKCOIN_WEBSOCKET_URL = "wss://real.okcoin.com:10440/websocket/okcoinapi"

# UTC ALL THE TIME, FOREVER AND EVER. 
TIMEZONE = pytz.timezone('UTC')

# ***** CHANGE THIS TO BE THE URL OF YOUR ELASTICSEARCH SERVER *****
ELASTICSEARCH_HOST = "http://localhost:9200"

# Default index name in elasticsearch to use for the btc_usd market data aggregation
DEFAULT_INDEX_NAME = "btcwebsockettickerarchive"

es = None

## TODO: RESTRUCTURE THIS INTO MORE MEANINGFUL CLASSES


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
							"count": {"type": "float"}, 
							"amount": {"type": "float"} 
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
		es.indices.create(DEFAULT_INDEX_NAME)
		es.indices.put_mapping(index=DEFAULT_INDEX_NAME, doc_type="bitfinex", body=bitfinexMapping)
		es.indices.put_mapping(index=DEFAULT_INDEX_NAME, doc_type="okcoin", body=okcoinMapping)
		es.indices.put_mapping(index=DEFAULT_INDEX_NAME, doc_type="bitfinex_order_book", body=bitfinexOrderBookMapping)

		mappingCreated = True
	except: 
		pass
	return mappingCreated

def on_open(self):
    self.send("{'event':'addChannel','channel':'ok_btcusd_ticker','binary':'true'}")

def on_message(self, event):
    okcoinData = inflate(event) #data decompress
    injectTickerData(okcoinData)

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
	putNewDocumentRequest = es.create(index=DEFAULT_INDEX_NAME, doc_type='okcoin', ignore=[400], id=uniqueId, body=okCoinDto)
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
    print (event)

def on_close(self, event):
    print (event)

if __name__ == "__main__":
	es = elasticsearch.Elasticsearch([ELASTICSEARCH_HOST])
	createMappings(es)
	websocket.enableTrace(False)
	ws = websocket.WebSocketApp(OKCOIN_WEBSOCKET_URL, on_message = on_message, on_error = on_error, on_close = on_close)
	ws.on_open = on_open
	ws.run_forever()

