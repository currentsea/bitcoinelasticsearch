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
		try: 
			self.es = elasticsearch.Elasticsearch([self.esUrl])
		except: 
			raise
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
					"volume": {"type" : "float"}, 
					"absolute_volume": { "type": "float"},
					"order_type": { "type": "string"}
				}
			}
		}
		return orderbookMapping

	def getCompletedTradesMapping(self): 
		completedTradeMapping = { 
			"bitfinex": { 
				"properties": { 
					"uuid": { "type": "string", "index": "no" }, 
					"date" : { "type": "date" }, 
					"sequence_id": { "type" : "string", "index":"not_analyzed"}, 
					"price": {"type": "float"}, 
					"volume": {"type": "float"},
					"order_type" : { "type": "string"}, 
					"absolute_volume" : {"type":"string"},
					"currency_pair": {"type":"string"}
				 }
			}
		} 
		return completedTradeMapping



	def getOrderDto(self, dataSet, currencyPair): 
		if (len(dataSet) != 3): 
			raise IOError("Invalid data set passed to getOrderDto") 
		orderDto = {}
		recordDate = datetime.datetime.now(TIMEZONE)
		uuidVar = uuid.uuid4()
		uuidStr = str(uuidVar) 
		orderDto["uuid"] = uuidStr
		orderDto["date"] = recordDate 
		orderDto["currency_pair"] = currencyPair
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

	def completedTradeDto(self, completedTrade, currencyPair): 
		tradeDto = {}
		recordDate = datetime.datetime.now(TIMEZONE)
		uuidVar = uuid.uuid4()
		uuidStr = str(uuidVar) 
		pass

	# if len(completedTrade) == 4: 
	# 	tradeDto["sequence_id"] = str(completedTrade[0])
	# 	# tradeDto["timestamp"] = str(completedTrade[1])
	# 	tradeDto["price"] = float(completedTrade[2]) 
	# 	theAmount = float(completedTrade[3])
	# 	tradeDto["amount"] = theAmount
		
	# elif len(completedTrade) == 5: 
	# 	tradeDto["tradeId"] = str(completedTrade[1])
	# 	tradeDto["timestamp"] = str(completedTrade[2])
	# 	tradeDto["price"] = float(completedTrade[3]) 
	# 	theAmount = float(completedTrade[4]) 
	# 	tradeDto["amount"] = theAmount

	# elif len(completedTrade) == 6: 
	# 	tradeDto["sequenceId"] = str(completedTrade[1])
	# 	tradeDto["tradeId"] = str(completedTrade[2])
	# 	tradeDto["timestamp"] = str(completedTrade[3])
	# 	tradeDto["price"] = float(completedTrade[4]) 
	# 	theAmount = float(completedTrade[5]) 
	# 	tradeStatusType = completedTrade[1]
	# 	if tradeStatusType['te']:
	# 		pass
	# 	elif tradeStatusType['tu']:
	# 		print ("TRADE STATUS TYPE IS tu")
	# 	tradeDto["uuid"] = uuidStr
	# 	tradeDto["date"] = recordDate 
	# 	tradeDto["volume"] = float(completedTrade[5])

	def postDto(self, orderDto, indexName=DEFAULT_INDEX_NAME, docType=DEFAULT_DOCTYPE_NAME): 
		self.connectElasticsearch()
		try: 
			es.indices.create(name)
			try: 
				mappingDto = getOrderbookElasticsearchMapping()
				es.indices.put_mapping(index=indexName, doc_type=docType, body=mappingDto)
				print ("Created mappings for " + str(docType))
			except: 
				pass
		except: 
			pass
		newDocUploadRequest = self.es.create(index=indexName, doc_type=docType, ignore=[400], id=uuid.uuid4(), body=orderDto)
		return newDocUploadRequest["created"]


	def connectOrderbookSocket(self): 
		# connectWebsocket()
		symbols = self.getSymbols()
		self.connectWebsocket()
		print ("ATTEMPTING TO CONNECT " + str(len(symbols)) + " CURRENCY PAIRS TO THE ORDERBOOK FEED...")
# 		   "event": "subscribe",
#     "channel": "book",
#     "pair": "BTCUSD",
#     "prec": "P0",
#     "len":"<LENGTH>"
# }))

		try: 
			for symbol in symbols: 
				self.ws.send(json.dumps({
					"event": "subscribe",
				    "channel": "book",
				    "pair": symbol,
				    "prec": "P0",
				    "len":"100"	
				}))
				self.ws.send(json.dumps({ 
					"event": "subscribe", 
					"channel": "trades", 
					"pair": symbol
				}))
				self.ws.send(json.dumps({ 
				    "event": "subscribe",
				    "channel": "ticker",
				    "pair": symbol
				}))
				print ("SUCCESSFULLY CONNECTED " + symbol + " TO ORDERBOOK FEED!")
				sys.stdout.flush()
		except: 
			raise
		print ("FINISHED CONNECTING CURRENCY PAIRS TO DATA FEED SUCCESSFULLY.") 
		channelDict = {}
		channelMappings = {}
		allChannelsSubscribed = False
		addedOrderbookDocs = 0
		while (True):
			resultData = self.ws.recv()
			try: 
				dataJson = json.loads(resultData)
				if ("chanId" in dataJson and "event" in dataJson and allChannelsSubscribed == False): 
					# print (dataJson)
					pairName = str(dataJson["pair"])
					print ("PAIR NAME IS: " + pairName)
					pairChannelType = str(dataJson["channel"])
					identifier = pairName
					channelId = dataJson["chanId"]
					channelDict[channelId] = identifier
					channelMappings[channelId] = dataJson
					if (len(channelDict) == len(symbols * 3)): 
						allChannelsSubscribed = True
						print ("all channels subscribed..") 
				else: 
					print ("^__^") 
					theResult = list(dataJson)
					print ("")
					print (channelMappings)
					print ("")
					print (theResult)
					try: 
						curChanId = int(theResult[0])
						print (curChanId in channelMappings)
					except ValueError: 
						pass 
					except: 
						raise
					try: 
						chanId = int(theResult[0])
						# dtoType = str(channelDict[chanId])
						dtoType = str(channelMappings[chanId]["pair"])
						channelType = str(channelMappings[chanId]["channel"])
						if channelType == "book": 	
							if len(dataJson) == 2:
								orderList = theResult[1]
								if orderList == 'hb': 
									print ("SKIP (heartbeat)") 
								else: 
									for orderItem in orderList: 
										orderDto = self.getOrderDto(orderItem, dtoType)
										postedDto = self.postDto(orderDto)
										if postedDto == False: 
											raise IOError("Unable to add new document to ES..." )
										else: 
											if addedOrderbookDocs % 1000 == 0:
												print (str(addedOrderbookDocs) + " Orderbook Entries Added So Far This Run...") 
											addedOrderbookDocs = addedOrderbookDocs + 1 

							elif len(dataJson) == 4: 
								dataSet = dataJson[1:]
								print (dtoType)
								curDto = self.getOrderDto(dataSet, dtoType)
								postedDto = self.postDto(curDto)
								if postedDto == False: 
									raise IOError("Unable to add new document to ES..." )
								else: 
									if addedOrderbookDocs % 1000 == 0:
										print (str(addedOrderbookDocs) + " Orderbook Entries Added So Far This Run...") 
									addedOrderbookDocs = addedOrderbookDocs + 1 

							else: 
								raise IOError("Invalid orderbook item") 
						else: 
							print ("Channel with type: " + channelType + " is not yet supported")
					except: 
						print ("") 
						print ('HORSE SHIT') 
						print (resultData)
			except: 
				raise 
