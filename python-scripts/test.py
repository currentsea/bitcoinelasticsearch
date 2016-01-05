import json, uuid, datetime, pytz, elasticsearch
from websocket import create_connection

# Default index name in elasticsearch to use for the btc_usd market data aggregation
DEFAULT_INDEX_NAME = "btcwebsockettickerarchive"

# UTC ALL THE TIME, FOREVER AND EVER. 
TIMEZONE = pytz.timezone('UTC')

# ***** CHANGE THIS TO BE THE URL OF YOUR ELASTICSEARCH SERVER *****
ELASTICSEARCH_HOST = "http://localhost:9200"

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
		es.indices.create(DEFAULT_INDEX_NAME)
		es.indices.put_mapping(index=DEFAULT_INDEX_NAME, doc_type="bitfinex", body=bitfinexMapping)
		es.indices.put_mapping(index=DEFAULT_INDEX_NAME, doc_type="okcoin", body=okcoinMapping)
		mappingCreated = True
	except: 
		pass
	return mappingCreated

if __name__ == "__main__": 

	es = elasticsearch.Elasticsearch([ELASTICSEARCH_HOST])
	createMappings(es)
	ws = create_connection("wss://api2.bitfinex.com:3000/ws")

	ws.send(json.dumps({
	    "event": "subscribe",
	    "channel": "ticker",
	    "pair": "BTCUSD"
	}))


	while True:
		result = ws.recv()
		result = json.loads(result)
		if (len(result) == 11): 
			uniqueId = str(uuid.uuid4())
			recordDate = datetime.datetime.now(TIMEZONE)
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
			print (bitfinexTickerDict)
			putNewDocumentRequest = es.create(index=DEFAULT_INDEX_NAME, doc_type='bitfinex', ignore=[400], id=uniqueId, body=bitfinexTickerDict)
			print (putNewDocumentRequest)
			successful = putNewDocumentRequest["created"]
			if successful == True: 
				print("WEBSOCKET ENTRY ADDED TO ES CLUSTER")
			else: 
				print("!! FATAL !!: WEBSOCKET ENTRY NOT ADDED TO ES CLUSTER")


	ws.close()