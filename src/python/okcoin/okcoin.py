#!/usr/bin/python3
__author__ = "currentsea"
__copyright__   = "Copyright 2016, currentsea"
__license__ = "MIT"

import os, websocket, time, datetime, sys, json, hashlib, zlib, base64, json, re, elasticsearch, argparse, uuid, pytz


DEFAULT_DOCTYPE_NAME = "okcoin"
DEFAULT_INDEX_NAME = "live_crypto_orderbooks"
DEFAULT_WEBSOCKETS_URL = "wss://real.okcoin.com:10440/websocket/okcoinapi"
DEFAULT_ELASTICSEARCH_URL = "http://localhost:9200"
TIMEZONE = pytz.timezone("UTC")
DEFAULT_INDECES = ["live_crypto_orderbooks", "live_crypto_tickers", "live_crypto_trades"]
TIMEZONE = pytz.timezone('UTC')

class Okcoin(): 
	def __init__(self, wsUrl=DEFAULT_WEBSOCKETS_URL, esUrl=DEFAULT_ELASTICSEARCH_URL):
		self.wsUrl = wsUrl
		self.esUrl = esUrl
		self.connectElasticsearch()
		self.createIndices()

		# self.symbols = self.getSymbols()
		# self.connectWebsocket()
		# self.connectElasticsearch()
		# self.createIndices()
		# self.getCompletedTradesMapping()
		# self.getOrderbookElasticsearchMapping()
		# self.createMappings()
	def run(self): 
		websocket.enableTrace(False)
		ws = websocket.WebSocketApp(self.wsUrl, on_message = self.websocketMessage, on_error = self.websocketError, on_close = self.websocketClose)
		ws.on_open = self.subscribePublicChannels
		ws.run_forever()

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

	def subscribePublicChannels(self, connector):
		connector.send("{'event':'addChannel','channel':'ok_btcusd_ticker','binary': 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_btcusd_depth', 'binary': 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_btcusd_trades_v1', 'binary': 'true'}")
		connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_1min', 'binary':'true'}")
		connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_3min', 'binary':'true'}")
		connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_5min', 'binary':'true'}")
		connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_15min', 'binary':'true'}")
		connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_30min', 'binary':'true'}")
		connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_1hour', 'binary':'true'}")
		connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_2hour', 'binary':'true'}")
		connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_4hour', 'binary':'true'}")
		connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_6hour', 'binary':'true'}")
		connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_12hour', 'binary':'true'}")
		connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_day', 'binary':'true'}")
		connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_3day', 'binary':'true'}")
		connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_week', 'binary':'true'}")
		connector.send("{'event':'addChannel','channel':'ok_btcusd_future_ticker_this_week', 'binary': 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_btcusd_future_ticker_next_week', 'binary': 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_btcusd_future_ticker_quarter', 'binary': 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_btcusd_future_index', 'binary':'true'}")

	def inflate(self, okcoinData):
	    decompressedData = zlib.decompressobj(-zlib.MAX_WBITS)
	    inflatedData = decompressedData.decompress(okcoinData)
	    inflatedData += decompressedData.flush()
	    return inflatedData

	def websocketError(self, event):
		print('ERROR IS: ') 
		print (event)

	def websocketClose(self, event):
	    print (event)

	def websocketMessage(self, connection, event):
		okcoinData = self.inflate(event) #data decompress
		print (okcoinData)
		# jsonData = getJsonData(okcoinData)
		# print (jsonData)
		# for item in jsonData: 
		# 	curChannel = item["channel"]
		# 	# if curChannel == "ok_btcusd_ticker": 
		# 	# 	self.injectTickerData(self, event, item)
		# 	# elif curChannel == "ok_btcusd_depth": 
		# 	# 	processOrderbook(self, event, item) 
		# 	# elif curChannel == "ok_btcusd_trades_v1": 
		# 	# 	processCompletedTrades(jsonData)
		# 	# elif curChannel in CANDLE_LIST: 
		# 	# 	processCandleStick(curChannel, item)
		# 	# elif curChannel in FUTURES_CONTRACT_TYPES: 
		# 	# 	processTheFuture(curChannel, item) 
		# 	# elif curChannel == "ok_btcusd_future_index": 
		# 	# 	indexTheFuture(curChannel, item)
		# 	# else: 
		# 	# 	print("WTF")
		# 	print(curChannel)
		print (connection)
		print("-----") 
		pass
