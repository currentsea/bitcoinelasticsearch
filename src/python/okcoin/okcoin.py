#!/usr/bin/python3
__author__ = "currentsea"
__copyright__   = "Copyright 2016, currentsea"
__license__ = "MIT"

import os, websocket, time, datetime, sys, json, hashlib, zlib, base64, json, re, elasticsearch, argparse, uuid, pytz


DEFAULT_DOCTYPE_NAME = "okcoin"
DEFAULT_INDEX_NAME = "live_crypto_orderbooks"
DEFAULT_WEBSOCKETS_URL = "wss://real.okcoin.com:10440/websocket/okcoinapi"
DEFAULT_ELASTICSEARCH_URL = "https://search-bitcoins-2sfk7jzreyq3cfjwvia2mj7d4m.us-west-2.es.amazonaws.com" 
TIMEZONE = pytz.timezone("UTC")
DEFAULT_INDECES = ["live_crypto_orderbooks", "live_crypto_tickers", "live_crypto_trades"]
TIMEZONE = pytz.timezone('UTC')

class Okcoin(): 
	def __init__(self, wsUrl=DEFAULT_WEBSOCKETS_URL, esUrl=DEFAULT_ELASTICSEARCH_URL):
		self.wsUrl = wsUrl
		self.esUrl = esUrl
		self.connectElasticsearch()
		self.createIndices()
		self.getTickerMapping()
		self.getCompletedTradesMapping()
		self.getOrderbookElasticsearchMapping()
		self.createMappings()
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
		connector.send("{'event':'addChannel','channel':'ok_sub_spotusd_btc_ticker','binary': 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_sub_spotusd_ltc_ticker','binary': 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_sub_spotusd_btc_depth_60', 'binary': 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_sub_spotusd_ltc_depth_60', 'binary': 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_sub_spotusd_btc_trades'}");
		connector.send("{'event':'addChannel','channel':'ok_sub_spotusd_ltc_trades'}");

		# connector.send("{'event':'addChannel','channel':'ok_btcusd_trades_v1', 'binary': 'true'}")
		# connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_1min', 'binary':'true'}")
		# connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_3min', 'binary':'true'}")
		# connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_5min', 'binary':'true'}")
		# connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_15min', 'binary':'true'}")
		# connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_30min', 'binary':'true'}")
		# connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_1hour', 'binary':'true'}")
		# connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_2hour', 'binary':'true'}")
		# connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_4hour', 'binary':'true'}")
		# connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_6hour', 'binary':'true'}")
		# connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_12hour', 'binary':'true'}")
		# connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_day', 'binary':'true'}")
		# connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_3day', 'binary':'true'}")
		# connector.send("{'event':'addChannel', 'channel': 'ok_btcusd_kline_week', 'binary':'true'}")
		# connector.send("{'event':'addChannel','channel':'ok_btcusd_future_ticker_this_week', 'binary': 'true'}")
		# connector.send("{'event':'addChannel','channel':'ok_btcusd_future_ticker_next_week', 'binary': 'true'}")
		# connector.send("{'event':'addChannel','channel':'ok_btcusd_future_ticker_quarter', 'binary': 'true'}")
		# connector.send("{'event':'addChannel','channel':'ok_btcusd_future_index', 'binary':'true'}")

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

	def getOrderbookElasticsearchMapping(self):
		self.orderbookMapping = {
			"okcoin": {
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
			"okcoin": {
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
			"okcoin": {
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
		return self.tickerMapping

	def getTickerDto(self, dataSet, currencyPair): 
		dto = {}
		dto["uuid"] = str(uuid.uuid4())
		dto["date"] = datetime.datetime.now(TIMEZONE)
		dto["volume"] = float(str(dataSet["vol"].replace(",","")))
		dto["timestamp"] = str(dataSet["timestamp"])
		dto["last_price"] = float(dataSet["last"])
		dto["low_price"] = float(dataSet["low"])
		dto["ask"] = float(dataSet["sell"])
		dto["bid"] = float(dataSet["buy"])
		dto["high"] = float(dataSet["high"])
		dto["currency_pair"] = str(currencyPair)
		return dto

	def postDto(self, dto, indexName=DEFAULT_INDEX_NAME, docType=DEFAULT_DOCTYPE_NAME):
		newDocUploadRequest = self.es.create(index=indexName, doc_type=docType, ignore=[400], id=uuid.uuid4(), body=dto)
		return newDocUploadRequest["created"]

	def getDepthDtoList(self, dataSet, currencyPair): 
		dtoList = []
	
		print ("------")
		for bid in dataSet["bids"]: 
			dto = {}
			if len(bid) == 2: 
				dto["uuid"] = str(uuid.uuid4())
				dto["date"] = datetime.datetime.now(TIMEZONE)
				dto["currency_pair"] = str(currencyPair)
				dto["timestamp"] = str(dataSet["timestamp"])
				dto["price"] = float(bid[0])
				volumeVal = float(bid[1]) 
				dto["volume"] = float(volumeVal)
				dto["absolute_volume"] = float(volumeVal)
				dto["order_type"] = "BID"
				dto["count"] = float(1)
				dtoList.append(dto)
		for ask in dataSet["asks"]: 
			dto = {}
			if len(ask) == 2: 
				dto["uuid"] = str(uuid.uuid4())
				dto["date"] = datetime.datetime.now(TIMEZONE)
				dto["currency_pair"] = str(currencyPair)
				dto["timestamp"] = str(dataSet["timestamp"])
				dto["price"] = float(ask[0])
				volumeVal = float(ask[1])
				volumeVal = volumeVal * -1
				dto["volume"] = float() 
				dto["absolute_volume"] = float(volumeVal)
				dto["order_type"] = "ASK"
				dto["count"] = float(1)
				dtoList.append(dto)
		print ("------")
		return dtoList

	def getCompletedTradeDtoList(self, dataSet, currencyPair): 
		dtoList = []

		print (dataSet)
# [tid, price, amount, time, type]
		for completedTrade in dataSet: 
			dto = {}
			dto["order_id"] = str(completedTrade[0])
			dto["price"] = float(completedTrade[1])
			dto["uuid"] = str(uuid.uuid4())
			dto["date"] = datetime.datetime.now(TIMEZONE)
			dto["currency_pair"] = str(currencyPair)
			absVol = float(completedTrade[2])
			dto["absolute_volume"] = float(absVol)
			timestamp = str(completedTrade[3])
			orderType = str(completedTrade[4])
			orderType = orderType.upper()
			dto["order_type"] = orderType
			if orderType == "BID": 
				volumeVal = absVol * -1
				dto["volume"] = float(volumeVal)
			elif orderType == "ASK": 
				volumeVal = absVol
				dto["volume"] = float(volumeVal)
			else: 
				raise IOError("WTF order type is not ask or bid for completed trade")
			dto["timestamp"] = str(timestamp)
			dtoList.append(dto)

		return dtoList

	def websocketMessage(self, connection, event):
		okcoinData = self.inflate(event) #data decompress
		jsonData = self.getJsonData(okcoinData)
		for dataSet in jsonData: 
		 	curChannel = dataSet["channel"]
		 	if curChannel ==  "ok_sub_spotusd_btc_ticker": 
		 		dto = self.getTickerDto(dataSet["data"], "BTCUSD") 
		 		self.postDto(dto, "live_crypto_tickers")
	 		elif curChannel == "ok_sub_spotusd_ltc_ticker": 
		 		dto = self.getTickerDto(dataSet["data"],  "LTCUSD")
		 		self.postDto(dto, "live_crypto_tickers")
	 		elif curChannel == "ok_sub_spotusd_btc_depth_60": 
	 			dtoList = self.getDepthDtoList(dataSet["data"], "BTCUSD")
	 			for dto in dtoList: 
			 		self.postDto(dto, "live_crypto_orderbooks")
 			elif curChannel == "ok_sub_spotusd_ltc_depth_60": 
	 			dtoList = self.getDepthDtoList(dataSet["data"], "LTCUSD")
	 			for dto in dtoList: 
			 		self.postDto(dto, "live_crypto_orderbooks")
	 		elif curChannel == "ok_sub_spotusd_btc_trades": 
	 			print ('A TRADE')
	 			completedTradeDtoList = self.getCompletedTradeDtoList(dataSet["data"], "BTCUSD")
	 			for dto in completedTradeDtoList: 
			 		self.postDto(dto, "live_crypto_trades")
 			elif curChannel == "ok_sub_spotusd_ltc_trades": 
	 			completedTradeDtoList = self.getCompletedTradeDtoList(dataSet["data"], "LTCUSD")
	 			print (completedTradeDtoList)
	 			for dto in completedTradeDtoList: 
			 		self.postDto(dto, "live_crypto_trades")
		pass

	def getJsonData(self, okcoinData): 
		tempData = okcoinData
		dataStr = tempData.decode(encoding='UTF-8')
		jsonData =json.loads(dataStr)
		return jsonData

