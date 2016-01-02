#!/usr/bin/python
__author__ = "donnydevito"
__copyright__   = "Copyright 2015, donnydevito"
__license__ = "MIT"

import requests, json, re, uuid, datetime, argparse, warnings, pytz
from elasticsearch import Elasticsearch
from time import sleep 

# ***** CHANGE THIS TO BE THE URL OF YOUR ELASTICSEARCH SERVER *****
ELASTICSEARCH_HOST = "http://localhost:9200"

# Root URL that the bitfinex API uses (most likely wont change until (if ever) a /v2/../vN/ are released)
BITFINEX_API_ROOT = "https://api.bitfinex.com/v1"

# Bitfinex API time limit in seconds 
BITFINEX_API_TIME_LIMIT = 1

# Default index name in elasticsearch to use for the btc_usd market data aggregation
BITFINEX_DEFAULT_TICKER_INDEX_NAME = "btcusdmarketdata"

def getArgs(): 
	parser = argparse.ArgumentParser(description='BTC elastic search data collector')
	parser.add_argument('--forever', action='store_true', default=False)
	parser.add_argument('--max_records', action='store_true', default=3600)
	args = parser.parse_args()
	return args

def getBitfinexTickerData(tickerSymbol): 
	tickerDict = None
	req = requests.get(BITFINEX_API_ROOT + "/pubticker/" + tickerSymbol)
	# sleep(0.5)
	# bookReq = requests.get(BITFINEX_API_ROOT + "/book/" + tickerSymbol)
	if req.status_code < 400: 
		tickerDict = req.json()
		# tickerDict["order_book"] = bookReq.json()
	else: 
		print("REQUEST TO BITFINEX API FAILED: status code " + str(req.status_code))
	return tickerDict

def createMappings(es): 
	mappingCreated = False
	try: 
		bitfinexMapping = {
		    "bitfinex": {
		        "properties": {
		            "date": {"type": "date"},
		            "last_price": {"type": "float"},
		            "timestamp": {"type": "string", "index": "not_analyzed"},
		            "volume": {"type": "float"},
		            "mid": {"type": "float"},
		            "high": {"type": "float"},
		            "ask": {"type": "float"},
		            "low": {"type": "float"},
		            "bid": {"type": "float"}
		            # "order_book": {"type": "nested"}
		        }
		    }
		}
		es.indices.create(BITFINEX_DEFAULT_TICKER_INDEX_NAME)
		es.indices.put_mapping(index=BITFINEX_DEFAULT_TICKER_INDEX_NAME, doc_type="bitfinex", body=bitfinexMapping)
		mappingCreated = True
	except: 
		pass
	return mappingCreated

# Adds ticker item to elasticsearch 
# Uses a random UUID as the ID, can be analyzed via the timestamp in kibana
def addItem(es): 
	successful = False
	try: 
		tickerData = getBitfinexTickerData("BTCUSD")
		esPostUrl = ELASTICSEARCH_HOST + "/BITFINEX_DEFAULT_TICKER_INDEX_NAME/btcdata/"
		timezone = pytz.timezone('UTC')
		dateQueried = datetime.datetime.fromtimestamp(float(tickerData["timestamp"]), timezone)
		tickerData["date"] = dateQueried
		putNewDocumentRequest = es.create(index=BITFINEX_DEFAULT_TICKER_INDEX_NAME, doc_type='bitfinex', ignore=[400], id=uuid.uuid4(),body=tickerData)
		successful = putNewDocumentRequest["created"]
	except: 
		pass
	return successful

def updateIndex(es): 
	indexUpdated = addItem(es)
	logTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

	if indexUpdated == True: 
		print "[" + logTime + "]: 1 document successfully added to the " + BITFINEX_DEFAULT_TICKER_INDEX_NAME + " index." 
	else: 
		print "[" + logTime + "]: document failed to be successfully added to the " + BITFINEX_DEFAULT_TICKER_INDEX_NAME + " index (API calls too frequent?)"
	sleep(BITFINEX_API_TIME_LIMIT)


if __name__ == "__main__": 
	args = getArgs()
	warnings.filterwarnings("ignore")
	print args.forever
	es = Elasticsearch([ELASTICSEARCH_HOST])
	mappingCreated = createMappings(es)
	if mappingCreated == True:
		print "Created ES Mapping " + BITFINEX_DEFAULT_TICKER_INDEX_NAME + " \n Begin data collection..."
 	else: 
		print "Elasticsearch mapping already existed.  \nContinuing data collection..."
	
	if args.forever == True: 
		while 1 == 1: 
			updateIndex(es)
	else: 
		try: 
			maxRecords = int(args.max_records)
		except TypeError:
			raise IOError("Must enter an integer value for max_records")
		if maxRecords == None or maxRecords <= 0: 
			raise IOError("--max_records must be a positive integer value")
		counter = 0
		while counter != maxRecords: 
			updateIndex(es)
			counter = counter + 1

