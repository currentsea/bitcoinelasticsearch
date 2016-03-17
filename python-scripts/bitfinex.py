#!/usr/bin/python3
# Author: Joseph Bull ("***Curren*cy*tsea***") 
# Email: joetbull@gmail.com (alternate: jtbull@uw.edu)
# # # #  Do not redistribute without permission # # # # 
__author__ = "Joseph 'currentsea' Bull"
__copyright__   = "Copyright 2016, seclorum"

import os
import sys
import json
import uuid
import pytz
import datetime
import argparse
import requests
import elasticsearch

from websocket import create_connection
from create_mappings import createMappings

DEFAULT_DOCTYPE_NAME = "bitfinex" 
DEFAULT_INDEX_NAME = "live_crypto_orderbooks"
DEFAULT_API_URL = "https://api.bitfinex.com/v1"
DEFAULT_WEBSOCKETS_URL = "wss://api2.bitfinex.com:3000/ws"
DEFAULT_ELASTICSEARCH_URL = "http://localhost:9200"
TIMEZONE = pytz.timezone("UTC") 

class Bitfinex(): 
	def __init__(self, wsUrl=DEFAULT_WEBSOCKETS_URL, esUrl=DEFAULT_ELASTICSEARCH_URL, apiUrl=DEFAULT_API_URL): 
		self.wsUrl = wsUrl
		self.esUrl = esUrl
		self.apiUrl = apiUrl

	def connectWebsocket(self): 
		try: 
			self.ws = create_connection(self.wsUrl)
		except: 
			raise 
		return True

	def connectElasticsearch(self): 
		if self.es == None: 
			try: 
				self.es = elasticsearch.Elasticsearch([self.esUrl])
			except: 
				raise
		else: 
			pass
		return True

	def getSymbols(self): 
		symbolsApiEndpoint = self.apiUrl + "/symbols"
		print ("SYMBOLS ENDPOINT: " + symbolsApiEndpoint) 
		try: 
			req = requests.get(symbolsApiEndpoint)
			reqJson = req.json()
		except: 
			raise
		return reqJson

	def getOrderbookElasticsearchMapping(self): 
		orderbookMapping = {
			"bitfinex": {
				"properties": {
					"currency_pair": { "type": "string"}, 
					"uuid": { "type": "string", "index": "no"}, 
					"date": {"type": "date"},
					"price": {"type": "float"},
					"count": {"type": "float"},
					"volume" {"type" : "float"}, 
					"absolute_volume": { "type": "float"},
					"order_type": { "type": "string"}
				}
			}
		}
		return orderbookMapping

	def getOrderDto(self, dataSet, currencyPair): 
		if (len(dataSet) != 3): 
			raise IOError("Invalid data set passed to getOrderDto") 
		orderDto = {}
		recordDate = datetime.datetime.now(TIMEZONE)
		uuidVar = uuid.uuid4()
		uuidStr = str(uuidVar) 
		orderDto["uuid"] = uuidStr
		orderDto["date"] = recordDate 
		orderDto["price"] = float(dataSet[0])
		orderDto["count"] = float(dataSet[1]) 
		volVal = float(dataSet[2]) 
		orderDto["volume"] = volVal
		if volVal < 0: 
			orderDto["order_type"] = "ASK" 
			orderDto["absolute_volume"] = float(volVal * -1)
		else: 
			orderDto["order_type"] = "BID" 
			orderDto["absolute_volume"] = float(volVal)
		return orderDto

	def postOrderDto(self, orderDto, indexName=DEFAULT_INDEX_NAME, docType=DEFAULT_DOCTYPE_NAME): 
		try: 
			es.indices.create(name)
		except: 
			print "[INFO]: Index with name " + indexName + " already exists... Continuing..." 
		try: 
			mappingDto = getOrderbookElasticsearchMapping()
			es.indices.put_mapping(index=indexName, doc_type=docType, body=mappingDto)
			print "Created mappings for " + str(docType)
		except: 
			print "[INFO]: Mappings already existed.  Continuing..." 

		newDocUploadRequest = es.create(index=indexName, doc_type=docType, ignore=[400], id=uuid.uuid4(), body=orderDto)
		return newDocUploadRequest["created"]

	def connectOrderbookSocket(self): 
		# connectWebsocket()
		symbols = self.getSymbols()
		self.connectWebsocket()
		print ("ATTEMPTING TO CONNECT " + str(len(symbols)) + " CURRENCY PAIRS TO THE ORDERBOOK FEED...")
		try: 
			for symbol in symbols: 
				self.ws.send(json.dumps({
					"event": "subscribe",
				    "channel": "book",
				    "pair": symbol,
				    "prec": "P0",
				    "len":"100"	
				}))
				print ("SUCCESSFULLY CONNECTED " + symbol + " TO ORDERBOOK FEED!")
				sys.stdout.flush()
		except: 
			raise
		print ("FINISHED CONNECTING CURRENCY PAIRS TO DATA FEED SUCCESSFULLY.") 
		channelDict = {}
		allChannelsSubscribed = False
		while (True):
			resultData = self.ws.recv()
			try: 
				dataJson = json.loads(resultData)
				if ("chanId" in dataJson and allChannelsSubscribed == False): 
					# print (dataJson)
					pairName = str(dataJson["pair"])
					print ("PAIR NAME IS: " + pairName)
					pairChannelType = str(dataJson["channel"])
					identifier = pairName
					channelDict[dataJson["chanId"]] = identifier
					if (len(channelDict) == len(symbols)): 
						allChannelsSubscribed = True
						print ("all channels subscribed..") 
				else: 
					theResult = list(dataJson)
					chanId = theResult[0]
					if len(dataJson) == 2:
						nestedList = theResult[1]
					elif len(dataJson) == 4: 
						dtoType = str(channelDict[chanId])
						print ("single orderbook item")
						dataSet = dataJson[1:]
						print (dtoType)
						curDto = self.getOrderDto(dataSet, dtoType)
						postedDto = postOrderDto(curDto)
						if postedDto == False: 
							raise IOError("Unable to add new document to ES..." )
					else: 
						raise
			except: 
				raise


