# Author: Joseph Bull 
# Copyright 2016 
# Do Not Redistribute 
# Long Live Bitcoin! 

import argparse, hmac, hashlib, time, json, urllib, urllib2, requests, pytz, elasticsearch, poloinex, uuid, datetime
from time import sleep
ELASTICSEARCH_HOST = "https://search-bitcoins-2sfk7jzreyq3cfjwvia2mj7d4m.us-west-2.es.amazonaws.com/" 

def getArgs(): 
	parser = argparse.ArgumentParser(description='BTC elastic search data collector')
	parser.add_argument('--host', default=ELASTICSEARCH_HOST) 
	parser.add_argument('--forever', action='store_true', default=False)
	parser.add_argument('--max_records', action='store_true', default=3600)
	args = parser.parse_args()
	return args

def getIndeces(elasticHost): 
	pass

def getDtoList(connector, currencyPair="BTC_ETH"): 
	dtoList = []
	orderData = connector.returnOrderBook(currencyPair)

	bids = orderData["bids"]
	asks = orderData["asks"]

	for bid in bids: 
		dto = {}
		uniqueId = uuid.uuid4()
		dto["uuid"] = str(uniqueId)
		dto["date"] = datetime.datetime.utcnow()
		dto["currency_pair"] = currencyPair
		dto["order_type"] = "BID" 
		print bid 
		dto["price"] = float(bid[0])
		dto["volume"] = float(bid[1])
		dto["absolute_volume"] = float(bid[1])
		dtoList.append(dto)

	for ask in asks: 
		dto = {}
		uniqueId = uuid.uuid4()
		dto["uuid"] = str(uniqueId)
		dto["date"] = datetime.datetime.utcnow()
		dto["currency_pair"] = currencyPair
		dto["order_type"] = "ASK" 
		dto["price"] = float(bid[0])
		dto["volume"] = float(bid[1] * -1)
		dto["absolute_volume"] = float(bid[1])
		dtoList.append(dto)

	return dtoList 
	#return dtoList

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
			"poloinex_orderbook": {
				"properties": {
					"currency_pair": { "type": "string"},
					"uuid": { "type": "string"},
					"date": {"type": "date"},
					"order_type": { "type": "string"},
					"volume": { "type": "float"}, 
					"absolute_volume": { "type": "float"}, 
					"price" : {"type":"float"}
				} 
			} 
		}  

		es.indices.put_mapping(index=indexName, doc_type=docType, body=pnexTickersMapping)
	except: 
		raise 

def injectData(es, indexName, docType, docBody, conDocs): 	
	putNewDocumentRequest = es.create(index=indexName, doc_type=docType, ignore=[400], id=uuid.uuid4(), body=docBody)
	successful = putNewDocumentRequest["created"]
	if successful == True: 
		print "Added data for " + docType + " (docs consecutively added this run: " + str(conDocs) + ")"
	else: 
		raise

if __name__ == "__main__": 
	args = getArgs()
	indexName = "crypto_orderbooks" 
	docType = "poloinex_orderbook"
	#print args.host
	conDocs = 0
	es = elasticsearch.Elasticsearch(args.host, verify_certs=False) 
	createIndex(es, indexName) 
	putMapping(es, indexName, docType) 
	connector = poloinex.poloniex("", "")
	while 1 == 1: 
		orderData = getDtoList(connector)
		sleep(0.5)
		for dataPoint in orderData: 
			conDocs = conDocs + 1
			injectData(es, indexName, docType, dataPoint, conDocs)