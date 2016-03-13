# Author: Joseph "currentsea" Bull (joetbull@gmail.com) 
# DO NOT REDISTRIBUTE WITHOUT EXPRESS PERMISSION 
# Copyright 2016 Joseph Bull 
# Properietary & Confidential 

import requests, json, argparse, uuid, pytz, datetime, elasticsearch
from time import sleep 
TIMEZONE = pytz.timezone('UTC')
KRAKEN_API_HOST = "https://api.kraken.com"
ELASTICSEARCH_HOST = "https://search-bitcoins-2sfk7jzreyq3cfjwvia2mj7d4m.us-west-2.es.amazonaws.com"
DEFAULT_INDEX = "crypto_orderbooks"
DEFAULT_DOC_TYPE = "kraken_ethereum"
SLEEP_INTERVAL = 7.5
def getArgs(): 
	parser = argparse.ArgumentParser(description='BTC elastic search data collector')
	parser.add_argument('--host')
	parser.add_argument('--currency', default="ETHUSD")
	parser.add_argument('--forever', action='store_true', default=False)
	parser.add_argument('--max_records', action='store_true', default=3600)

	# TODO: add more params here

	args = parser.parse_args()
	return args

# /0/public/Depth
def getOrderbook(currencyPair): 
	endpoint = "/0/public/Depth"
	params = {}
	params["pair"] = currencyPair
	reqUrl = KRAKEN_API_HOST + endpoint
	req = requests.get(reqUrl, params=params)
	#  asks = ask side array of array entries(<price>, <volume>, <timestamp>)
	#  bids = bid side array of array entries(<price>, <volume>, <timestamp>)
	orderbook = {}
	reqResult = req.json()['result']
	keyCount = 0
	currencyKey = ""
	for fullKey in reqResult: 
		currencyKey = str(fullKey) 
		keyCount = keyCount + 1
	if keyCount > 1: 
		raise IOError("More than one currency pair returned in REST call") 
	elif keyCount == 0:
		raise IOError("Keycount cannot be 0") 
	else:  
		orderbook["bids"] = reqResult[currencyKey]["bids"]
		orderbook["asks"] = reqResult[currencyKey]["asks"]
	return orderbook

def getOrderbookMapping(): 
	krakenOrderbookMapping = {
		"kraken_orderbook": {
			"properties": {
					"uuid": { "type": "string", "index": "no"}, 
					"date": {"type": "date"},
					"currency_pair": { "type" : "string"}, 
					"timestamp": {"type": "string", "index": "no"}, 
					"price": {"type": "float"},
					"absolute_volume": {"type":"float"}, 
					"volume": {"type":"float"}, 
					"order_type": {"type": "string"}
			}
		}
	}
	return krakenOrderbookMapping

def getUniqueId(): 
	uniqueIdGen = uuid.uuid4()
	uniqueId = str(uniqueIdGen) 
	return uniqueId

def getCurrentDate(): 
	recordDate = datetime.datetime.now(TIMEZONE)
	return recordDate

def createOrderbookDto(orderbookData, orderType, displayPair="BTC_ETH"): 
	orderbookEntry = {}
	orderbookEntry["uuid"] = getUniqueId()
	orderbookEntry["date"] = getCurrentDate()
	if len(orderbookData) != 3: 
		raise IOError("Invalid object passed to create orderbook dto") 
	else: 
		volumeVal = float(orderbookData[1])
		orderbookEntry["price"] = float(orderbookData[0])
		orderbookEntry["absolute_volume"] = volumeVal
		orderbookEntry["timestamp"] = str(orderbookData[2])
		orderbookEntry["order_type"] = orderType
		orderbookEntry["currency_pair"] = str(displayPair)
		if orderType.upper() == "BID": 
			orderbookEntry["volume"] = float(volumeVal)
		elif orderType.upper() == "ASK": 
			newVal = volumeVal * -1 
			orderbookEntry["volume"] = float(newVal)
		else: 
			raise IOError("order_type must be either bid or ask") 
	return orderbookEntry

def getEntryQueue(orderbook): 
	entryList = []
	for bidOrder in orderbook["bids"]: 
		entryList.append(createOrderbookDto(bidOrder, "BID"))
	for askOrder in orderbook["asks"]: 
		entryList.append(createOrderbookDto(askOrder, "ASK"))
	return entryList

def initializeIndexConfiguration(es, mapping, indexName=DEFAULT_INDEX, docType=DEFAULT_DOC_TYPE): 
	try: 
		es.indices.create(DEFAULT_INDEX) 
	except: 
		print DEFAULT_INDEX + " already exists. Continuing..."
		pass 
	try: 
		es.indices.put_mapping(index=indexName, doc_type=docType, body=mapping)
	except: 
		print "Initial Index Config looks valid... Continuing"
		pass  

def injectEntry(entryData, indexName=DEFAULT_INDEX, docType=DEFAULT_DOC_TYPE): 
	putNewDocumentRequest = es.create(index=indexName, doc_type=docType, ignore=[400], id=uuid.uuid4(), body=entryData)
	successful = putNewDocumentRequest["created"]
	return successful

if __name__ == "__main__": 
	args = getArgs()
	orderbook = getOrderbook(args.currency)
	es = elasticsearch.Elasticsearch([ELASTICSEARCH_HOST], verify_ssl=True)
	mapping = getOrderbookMapping()
	initializeIndexConfiguration(es, mapping)

	count = 0 

	while 1 == 1: 
		orderbook = getOrderbook(args.currency) 
		orderEntries = getEntryQueue(orderbook) 
		for orderEntry in orderEntries: 
			successful = injectEntry(orderEntry) 
			if successful == True: 
				theId = orderEntry["uuid"]
				print "Successfully added orderbook entry with ID " + theId + " (order type: " + orderEntry["order_type"] + ") to ES cluster"
			else: 
				print "FAILED TO ADD " + theId + " to cluster..."  
		print "sleeping for " + str(SLEEP_INTERVAL) + " before requesting updated ledger..."
		sleep(SLEEP_INTERVAL)
