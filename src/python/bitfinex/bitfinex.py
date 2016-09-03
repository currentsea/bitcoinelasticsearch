#!/usr/bin/python3
# Author: Joseph Bull ("***Curren*cy*tsea***")
# Email: joetbull@gmail.com (alternate: jtbull@uw.edu)
# Program: bitfinex.py 
# Description: Wrapper around the poloinex exchange 
# Copyright (c) 2016 currentsea, Joseph Bull 

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

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

DEFAULT_DOCTYPE_NAME = "bitfinex"
DEFAULT_INDEX_NAME = "live_crypto_orderbooks"
DEFAULT_API_URL = "https://api.bitfinex.com/v1"
DEFAULT_WEBSOCKETS_URL = "wss://api2.bitfinex.com:3000/ws"
DEFAULT_ELASTICSEARCH_URL = "https://es.btcdata.org:9200"
TIMEZONE = pytz.timezone("UTC")
DEFAULT_INDECES = ["live_crypto_orderbooks", "live_crypto_tickers", "live_crypto_trades"]

class Bitfinex:
	def __init__(self, wsUrl=DEFAULT_WEBSOCKETS_URL, esUrl=DEFAULT_ELASTICSEARCH_URL, apiUrl=DEFAULT_API_URL):
		self.wsUrl = wsUrl
		self.esUrl = esUrl
		self.apiUrl = apiUrl
		self.symbols = self.getSymbols()
		self.connectWebsocket()
		self.connectElasticsearch()
		self.createIndices()
		self.getCompletedTradesMapping()
		self.getOrderbookElasticsearchMapping()
		self.createMappings()


	def createIndices(self, indecesList=DEFAULT_INDECES):
		for index in DEFAULT_INDECES:
			try:
				self.es.indices.create(index)
			except elasticsearch.exceptions.RequestError as e:
				print ("INDEX " + index + " ALREADY EXISTS")
			except:
				pass

	def createMappings(self):
		try:
			self.es.indices.put_mapping(index="live_crypto_orderbooks", doc_type=DEFAULT_DOCTYPE_NAME, body=self.orderbookMapping)
			self.es.indices.put_mapping(index="live_crypto_trades", doc_type=DEFAULT_DOCTYPE_NAME, body=self.completedTradeMapping)
			self.es.indices.put_mapping(index="live_crypto_tickers", doc_type=DEFAULT_DOCTYPE_NAME, body=self.orderbookMapping)
		except:
			raise

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
		self.orderbookMapping = {
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
		return self.orderbookMapping

	def getCompletedTradesMapping(self):
		self.completedTradeMapping = {
			"bitfinex": {
				"properties": {
					"uuid": { "type": "string", "index": "no" },
					"date" : { "type": "date" },
					"sequence_id": { "type" : "string", "index":"no"},
					"order_id":{  "type" : "string", "index":"no"},
					"price": {"type": "float"},
					"volume": {"type": "float"},
					"order_type" : { "type": "string"},
					"absolute_volume" : {"type":"float"},
					"currency_pair": {"type":"string"}, 
					"timestamp": { "type": "string", "index": "no"}
				 }
			}
		}
		return self.completedTradeMapping

	def getTickerMapping(self):
		self.tickerMapping = {
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
					"daily_change": {"type": "float"},
					"daily_delta": {"type" : "float"},
					"ask_volume": {"type": "float"},
					"bid_volume": {"type": "float"},
					"bid": {"type": "float"}, 
					"currency_pair": {"type":"string"}
				}
			}
		}
		return self.bitfinexTickerMapping

	def getOrderDto(self, dataSet, currencyPair):
		orderDto = {}
		recordDate = datetime.datetime.now(TIMEZONE)
		uuidVar = uuid.uuid4()
		uuidStr = str(uuidVar)
		orderDto["uuid"] = uuidStr
		orderDto["date"] = recordDate
		orderDto["currency_pair"] = currencyPair
		if (len(dataSet) != 3):
			print (dataSet)
			raise IOError("Invalid data set passed to getOrderDto")
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

	def getCompletedTradeDto(self, theResult, completedTrade, currencyPair):
		sliceData = theResult[1]

		if str(sliceData) == 'hb': 
			return None
		else: 
			tradeDto = {}
			recordDate = datetime.datetime.now(TIMEZONE)
			uuidVar = uuid.uuid4()
			uuidStr = str(uuidVar)
			tradeDto["uuid"] = str(uuidVar) 
			tradeDto["date"] = recordDate
			tradeDto["currency_pair"] = str(currencyPair)
	# print (theResult)
			if len(completedTrade) == 2: 
				sliceData = completedTrade[1]
				sliceList = []
				for data in sliceData: 
					if len(data) == 4: 
						tradeDataDto = {}
						tradeDataDto["uuid"] = str(uuidVar) 
						tradeDataDto["sequence_id"] = str(data[0])
						tradeDataDto["timestamp"] = str(data[1])
						tradeDataDto["price"] = float(data[2])
						volVal = data[3]
						tradeDataDto["volume"] = float(volVal)
						if volVal < 0: 
							absVol = volVal * -1
							tradeDataDto["order_type"] = "ASK" 
						else: 
							absVol = volVal
							tradeDataDto["order_type"] = "BID"
						tradeDataDto["absolute_volume"] = float(absVol)
						sliceList.append(tradeDataDto)
				return sliceList
						# print(data)
			else: 
				# [7, 'tu', '1494638-BTCUSD', 16304465, 1458389060, 407.66, 0.05]
				tradeDto = {}
				# raise IOError("completed trade result is length two with chan ID not length 2 heart beat or slice list")
				if len(completedTrade) >= 2: 
					sequenceType = completedTrade[1]
					if sequenceType == "te": 
						tradeDto["sequence_id"] = completedTrade[2]
						tradeDto["timestamp"] = completedTrade[3]
						tradeDto["price"] = float(completedTrade[4])
						volVal = float(completedTrade[5])
					elif sequenceType == "tu": 
						tradeDto["sequence_id"] = completedTrade[2]
						tradeDto["order_id"] = completedTrade[3]
						tradeDto["timestamp"] = completedTrade[4]
						tradeDto["price"] = completedTrade[5]
						volVal = float(completedTrade[6])
					else: 
						raise IOError("WEIRD") 
					tradeDto["volume"] = float(volVal)
					if volVal < 0: 
						absVol = volVal * -1
						tradeDto["order_type"] = "ASK" 
					else: 
						absVol = volVal
						tradeDto["order_type"] = "BID"
					tradeDto["absolute_volume"] = float(absVol)
				return tradeDto

	def postDto(self, dto, indexName=DEFAULT_INDEX_NAME, docType=DEFAULT_DOCTYPE_NAME):
		newDocUploadRequest = self.es.create(index=indexName, doc_type=docType, id=uuid.uuid4(), body=dto)
		return newDocUploadRequest["created"]

	def getChannelMappings(self):
		allChannelsSubscribed = False
		channelDict = {}
		channelMappings = {}
		for symbol in self.symbols:
			self.ws.send(json.dumps({
				"event": "subscribe",
			    "channel": "book",
			    "pair": symbol,
			    "prec": "P0",
			    "len":"100"
			}))
			self.ws.send(json.dumps({
				"event": "subscribe",
			    "channel": "ticker",
			    "pair": symbol,
			}))
			self.ws.send(json.dumps({
				"event": "subscribe",
			    "channel": "trades",
			    "pair": symbol,
			}))
		while (allChannelsSubscribed == False):
			resultData = self.ws.recv()
			try:
				dataJson = json.loads(resultData)
					# print (dataJson)
				pairName = str(dataJson["pair"])
				print ("PAIR NAME IS: " + pairName)
				pairChannelType = str(dataJson["channel"])
				identifier = pairName
				channelId = dataJson["chanId"]
				channelDict[channelId] = identifier
				channelMappings[channelId] = dataJson
				symbolLength = len(self.symbols)
				# CHANNELS ARE ALL SUBSCRIBED WHEN SYMBOL LENGTH * # # # # # # # # 
				targetLength = symbolLength * 3 
				# IF THIS SAVED YOU HOURS OF DEBUGGING, YOU'RE FUCKING WELCOME * #
				if (len(channelDict) == targetLength):
					allChannelsSubscribed = True
					print ("all channels subscribed..")
			except TypeError:
				pass
			except:
				raise
		return channelMappings

	def updateOrderBookIndex(self, theResult, dataJson, currencyPairSymbol):
		if len(dataJson) == 2:
			orderList = theResult[1]
			if orderList == 'hb':
				# print ("^^^^^^^^^^^^^WHO KNOCKS^^^^^^^^^^^^^^")
				pass
			else:
				for orderItem in orderList:
					orderDto = self.getOrderDto(orderItem, currencyPairSymbol)
					print (orderDto)
					postedDto = self.postDto(orderDto)
					if postedDto == False:
						raise IOError("Unable to add new document to ES..." )
		elif len(dataJson) == 4:
			dataSet = dataJson[1:]
			# print (currencyPairSymbol)
			curDto = self.getOrderDto(dataSet, currencyPairSymbol)
			postedDto = self.postDto(curDto)
			if postedDto == False:
				raise IOError("Unable to add new document to ES..." )
		else:
			raise IOError("Invalid orderbook item")


	def getTickerDto(self, theResult, tickerData, currencyPairSymbol): 
		tickerDto = {}
		recordDate = datetime.datetime.now(TIMEZONE)
		uuidVar = uuid.uuid4()
		uuidStr = str(uuidVar)
		tickerDto["uuid"] = str(uuidVar)
		tickerDto["date"] = recordDate
		tickerDto["currency_pair"] = currencyPairSymbol

		print (tickerData)
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
		tickerDto = {}
		tickerDto["last_price"] = lastPrice
		tickerDto["volume"] = volume
		tickerDto["high"] = highPrice
		tickerDto["ask"] = askPrice
		tickerDto["low"] = lowPrice
		tickerDto["bid"] = bidPrice
		tickerDto["daily_change"] = dailyChange
		tickerDto["daily_delta"] = dailyDelta
		tickerDto["ask_volume"] = askVol
		tickerDto["bid_volume"] = bidVol
		return tickerDto

	def run(self):
		try:
			resultData = self.ws.recv()
			self.channelMappings = self.getChannelMappings()
			while (True):
				# print ("^__^")
				resultData = self.ws.recv()
				dataJson = json.loads(resultData)
				theResult = list(dataJson)
				try:
					curChanId = int(theResult[0])
					# print (curChanId in self.channelMappings)
				except ValueError:
					pass
				except:
					raise
				try:
					hbCounter = 0
					chanId = int(theResult[0])
					# currencyPairSymbol = str(channelDict[chanId])
					currencyPairSymbol = str(self.channelMappings[chanId]["pair"])
					channelType = str(self.channelMappings[chanId]["channel"])
					heartbeat = [chanId, 'hb']
					if theResult == heartbeat:
						hbCounter = hbCounter + 1
						if hbCounter % 100 == 0: 
							print("HEARTBEAT COUNTER (Trades Channel): " + str(hbCounter))
					else: 
						if channelType == "book":
							self.updateOrderBookIndex(theResult, dataJson, currencyPairSymbol)
						elif channelType == "trades": 
							tradesDto = self.getCompletedTradeDto(theResult, dataJson, currencyPairSymbol)
							if type(tradesDto) is list: 
								for dto in tradesDto: 
									self.postDto(dto, "live_crypto_trades", DEFAULT_DOCTYPE_NAME)
							elif tradesDto != None: 
								self.postDto(tradesDto, "live_crypto_trades", DEFAULT_DOCTYPE_NAME)
							else: 
								hbCounter = hbCounter + 1
								if hbCounter % 10 == 0: 
									print ('HB INTERVAL OF 10!') 
						elif channelType == "ticker": 
							tickerDto = self.getTickerDto(theResult, dataJson, currencyPairSymbol)
							# print("OOOOOO")							
							# print("OOOOOO")
							# print("OOOOOO")
							# print("OOOOOO")
							# print("OOOOOO")
							print("OOOOOO")
							print("OOOOOO")
							print("OOOOOO")
							print("OOOOOO")
							print (self.postDto(tickerDto, "live_crypto_tickers", DEFAULT_DOCTYPE_NAME))
				except ValueError:
					pass
				except KeyError:
					self.channelMappings = self.getChannelMappings()
					print ("")
					print ("HORSE SHIT FROM CHANNEL: " + str(channelType))
					print (resultData)
					raise
		except:
			raise
