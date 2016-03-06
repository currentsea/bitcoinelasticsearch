# Author: Joseph Bull 
# Copyright 2016 
# Do Not Redistribute 
# Long Live Bitcoin! 

import argparse, hmac, hashlib, time, json, urllib, urllib2, requests, pytz, elasticsearch, poloinex, uuid, datetime
from time import sleep
ELASTICSEARCH_HOST = "http://127.0.0.1:9200" 

def getArgs(): 
	parser = argparse.ArgumentParser(description='BTC elastic search data collector')
	parser.add_argument('--host', default=ELASTICSEARCH_HOST) 
	parser.add_argument('--forever', action='store_true', default=False)
	parser.add_argument('--max_records', action='store_true', default=3600)
	args = parser.parse_args()
	return args

def getIndeces(elasticHost): 
	pass

def getDtoList(connector): 
	dtoList = []
	tickerData = connector.returnTicker()
	for item in tickerData: 
		dto = {}
		uniqueId = uuid.uuid4()
		dto["uuid"] = str(uniqueId)
		dto["date"] = datetime.datetime.utcnow()
		dto["symbol"] = item
		itemDict = tickerData[item]
		for key in itemDict: 
			dto[key] = itemDict[key]
		dtoList.append(dto)
	return dtoList

def createIndex(es, name): 
	try: 
		es.indices.get(name)
	except elasticsearch.exceptions.NotFoundError: 
		# print ("Index " + name + " already existed on target host. Continuing...")
		# pass
		print es.indices.create(name)
	except:
		raise

def putMapping(es, indexName, docType): 
	try: 
		pnexTickersMapping = {
			"poloinex_ticker": {
				"properties": {
					"symbol": { "type": "string"},
					"uuid": { "type": "string"},
					"date": {"type": "date"},
					"last": {"type": "float"},
					"quoteVolume": {"type": "float"},
					"high24hr": {"type": "float"},
					"isFrozen": {"type": "string"},
					"highestBid": {"type": "float"},
					"percentChange": {"type": "float"},
					"low24hr": {"type": "float"},
					"lowestAsk": {"type": "float"},
					"baseVolume": {"type": "float"}
				} 
			} 
		}  
		es.indices.put_mapping(index=indexName, doc_type=docType, body=pnexTickersMapping)
	except: 
		#print ("MAPPING NOT UPDATED FOR " + indexName + " ON DOCTYPE poloinex")
		raise 

# def injectOrderBook(es, orderbook, uniqueId, recordDate, indexName="btc_orderbooks_live", docType="bitfinex_order_book"): 
	# for item in orderbook: 
	# 	orderDto = getOrderBookDto(item, uniqueId, recordDate)
	# 	successful = putNewDocumentRequest["created"]
	# 	if successful == True: 
	# 		print("[INDEX: " + indexName + "] [DOCTYPE: " + docType + "] Updated: " + uniqueId)  
	# 	else: 
	# 		print("ES Entry failed to POST: " + uniqueId)  


def injectData(es, indexName, docType, docBody, conDocs): 	
	putNewDocumentRequest = es.create(index=indexName, doc_type=docType, ignore=[400], id=uuid.uuid4(), body=docBody)
	successful = putNewDocumentRequest["created"]
	if successful == True: 
		print "Added data for " + docType + " (docs consecutively added this run: " + str(conDocs) + ")"
	else: 
		raise

if __name__ == "__main__": 
	args = getArgs()
	indexName = "crypto_tickers" 
	docType = "poloinex_ticker"
	#print args.host
	conDocs = 0
	es = elasticsearch.Elasticsearch(args.host, verify_certs=False) 
	createIndex(es, indexName) 
	putMapping(es, indexName, docType) 
	connector = poloinex.poloniex("", "")
	while 1 == 1: 
		tickerData = getDtoList(connector)
		sleep(0.5)
		for dataPoint in tickerData: 
			conDocs = conDocs + 1
			injectData(es, indexName, docType, dataPoint, conDocs)