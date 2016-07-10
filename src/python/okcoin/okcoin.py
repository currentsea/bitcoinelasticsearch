#!/usr/bin/python3
__author__ = "currentsea"
__copyright__   = "Copyright 2016, currentsea"
__license__ = "MIT"

import os, websocket, time, datetime, sys, json, hashlib, zlib, base64, json, re, elasticsearch, argparse, uuid, pytz


DEFAULT_DOCTYPE_NAME = "okcoin"
DEFAULT_INDEX_NAME = "live_crypto_orderbooks"
DEFAULT_WEBSOCKETS_URL = "wss://real.okcoin.com:10440/websocket/okcoinapi"
# DEFAULT_ELASTICSEARCH_URL = "https://search-bitcoins-2sfk7jzreyq3cfjwvia2mj7d4m.us-west-2.es.amazonaws.com" 
#DEFAULT_ELASTICSEARCH_URL = "https://search-btc-staging-temp-r4fnlxi76bsx3mhonfyxrawr3y.us-west-2.es.amazonaws.com"
DEFAULT_ELASTICSEARCH_URL = "https://es1.btcdream.ws:9200" 
TIMEZONE = pytz.timezone("UTC")
DEFAULT_INDECES = ["live_crypto_orderbooks", "live_crypto_tickers", "live_crypto_trades", "live_crypto_candlesticks", "live_crypto_futures_contracts"]
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
		self.getKlineMapping()
		self.getFutureTickerMapping()
		self.createMappings()
		
	def run(self): 
		websocket.enableTrace(False)
		ws = websocket.WebSocketApp(self.wsUrl, on_message = self.websocketMessage, on_error = self.websocketError, on_close = self.websocketClose, on_open = self.subscribePublicChannels)
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
			klineMapping = self.getKlineMapping()
			self.es.indices.put_mapping(index="live_crypto_candlesticks", doc_type=DEFAULT_DOCTYPE_NAME, body=self.klineMapping)
			self.es.indices.put_mapping(index="live_crypto_futures_contracts", doc_type=DEFAULT_DOCTYPE_NAME, body=self.futureMapping)
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
			self.es = elasticsearch.Elasticsearch([self.esUrl], verify_ssl=True)
		except:
			raise		

	def subscribePublicChannels(self, connector):
		connector.send("{'event':'addChannel','channel':'ok_sub_spotusd_btc_ticker','binary': 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_sub_spotusd_ltc_ticker','binary': 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_sub_spotusd_btc_depth_60', 'binary': 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_sub_spotusd_ltc_depth_60', 'binary': 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_sub_spotusd_btc_trades'}")
		connector.send("{'event':'addChannel','channel':'ok_sub_spotusd_ltc_trades'}");
		connector.send("{'event':'addChannel','channel':'ok_sub_spotusd_btc_trades'}");

		websocket.send("{'event':'addChannel','channel':'ok_sub_spotusd_X_kline_Y'}");
		klineList = []
		klineTypes = ['1min', '3min', '5min', '15min', '30min', '1hour', '2hour', '4hour', '6hour', '12hour', 'day', '3day', 'week']
		for klineType in klineTypes: 
			connector.send("{'event':'addChannel','channel':'ok_sub_spotusd_btc_kline_'" + klineType + "', 'binary':'true'}");
			klineList.append("ok_sub_spotusd_btc_kline_" + klineType)
			connector.send("{'event':'addChannel','channel':'ok_sub_spotusd_ltc_kline_'" + klineType + "', 'binary':'true'}");
			klineList.append("ok_sub_spotusd_ltc_kline_" + klineType)
		self.klineList = klineList
		print(klineList)
		klineChannels = ['ok_sub_spotusd_btc_kline_1min', 'ok_sub_spotusd_ltc_kline_1min', 'ok_sub_spotusd_btc_kline_3min', 'ok_sub_spotusd_ltc_kline_3min', 'ok_sub_spotusd_btc_kline_5min', 'ok_sub_spotusd_ltc_kline_5min', 'ok_sub_spotusd_btc_kline_15min', 'ok_sub_spotusd_ltc_kline_15min', 'ok_sub_spotusd_btc_kline_30min', 'ok_sub_spotusd_ltc_kline_30min', 'ok_sub_spotusd_btc_kline_1hour', 'ok_sub_spotusd_ltc_kline_1hour', 'ok_sub_spotusd_btc_kline_2hour', 'ok_sub_spotusd_ltc_kline_2hour', 'ok_sub_spotusd_btc_kline_4hour', 'ok_sub_spotusd_ltc_kline_4hour', 'ok_sub_spotusd_btc_kline_6hour', 'ok_sub_spotusd_ltc_kline_6hour', 'ok_sub_spotusd_btc_kline_12hour', 'ok_sub_spotusd_ltc_kline_12hour', 'ok_sub_spotusd_btc_kline_day', 'ok_sub_spotusd_ltc_kline_day', 'ok_sub_spotusd_btc_kline_3day', 'ok_sub_spotusd_ltc_kline_3day', 'ok_sub_spotusd_btc_kline_week', 'ok_sub_spotusd_ltc_kline_week']
		self.klineChannels = klineChannels
		for channel in self.klineChannels: 
			event = "{'event':'addChannel','channel':'" + channel + "', 'binary': 'true'}"
			connector.send(str(event))

		self.futureTypes = [ "this_week", "next_week", "quarter" ]
		
		futureChannels = []
		# fucking redudndant as mother fucking shit 

		for futureType in self.futureTypes: 
			btcChannel = "ok_sub_futureusd_btc_ticker_" + futureType
			btcEvent = "{'event':'addChannel','channel':'" + btcChannel + "', 'binary': 'true'}"
			ltcChannel = "ok_sub_futureusd_ltc_ticker_" + futureType
			ltcEvent = "{'event':'addChannel','channel':'" + ltcChannel + "', 'binary': 'true'}"
			connector.send(str(btcEvent))
			connector.send(str(ltcEvent))
			futureChannels.append(ltcChannel)
			futureChannels.append(btcChannel)
			print('SUBSCRIBED TO THE FUTURE')
		
		# futureChannels.append("ok_btcusd_future_ticker_this_week")
		# futureChannels.append("ok_btcusd_future_ticker_next_week")
		# futureChannels.append("ok_btcusd_future_ticker_quarter")
		# futureChannels.append("ok_btcusd_future_index")
		# futureChannels.append("ok_sub_futureusd_btc_trade_this_week")
		# futureChannels.append("ok_sub_futureusd_btc_trade_next_week") 
		# futureChannels.append("ok_sub_futureusd_btc_trade_quarter")
		# futureChannels.append("ok_sub_futureusd_ltc_trade_this_week")
		# futureChannels.append("ok_sub_futureusd_ltc_trade_next_week") 
		# futureChannels.append("ok_sub_futureusd_ltc_trade_quarter")
		self.futureChannels = futureChannels

		connector.send("{'event':'addChannel','channel':'ok_btcusd_future_ticker_this_week', 'binary' : 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_btcusd_future_ticker_next_week', 'binary' : 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_btcusd_future_ticker_quarter', 'binary' : 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_btcusd_future_index', 'binary':'true', 'binary' : 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_sub_futureusd_btc_trade_this_week', 'binary' : 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_sub_futureusd_btc_trade_next_week', 'binary' : 'true'}") 
		connector.send("{'event':'addChannel','channel':'ok_sub_futureusd_btc_trade_quarter', 'binary' : 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_sub_futureusd_ltc_trade_this_week', 'binary' : 'true'}")
		connector.send("{'event':'addChannel','channel':'ok_sub_futureusd_ltc_trade_next_week', 'binary' : 'true'}") 
		connector.send("{'event':'addChannel','channel':'ok_sub_futureusd_ltc_trade_quarter', 'binary' : 'true'}")
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


	def inflate(self, okcoinData):
	    decompressedData = zlib.decompressobj(-zlib.MAX_WBITS)
	    inflatedData = decompressedData.decompress(okcoinData)
	    inflatedData += decompressedData.flush()
	    return inflatedData

	def websocketError(self, event, data):
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


# 		ok_sub_futureusd_X_ticker_Y Subscribe Contract Market Price
# websocket.send("{'event':'addChannel','channel':'ok_sub_futureusd_X_ticker_Y'}");
		# value of Y is: 
	def getKlineMapping(self): 
		self.klineMapping = {
			"okcoin": {
				"properties": {
					"uuid": { "type": "string", "index": "no"},
					"date": {"type": "date"},
					"timestamp": {"type": "string", "index": "no"},
					"open_price": {"type": "float"},
					"highest_price": {"type": "float"},
					"lowest_price": {"type": "float"},
					"close_price": {"type": "float"},
					"volume": {"type": "float"},
					"currency_symbol": {"type": "string"}, 
					"contract_type": {"type": "string"}
				}
			}
		}
		return self.klineMapping

	def getFutureTickerMapping(self): 
		self.futureMapping = { 
			"okcoin" : { 
				"properties": { 
					"uuid": { "type": "string", "index": "no"},
					"date": {"type": "date"},
					"timestamp": {"type": "string", "index": "no"},	
					"buy_price": {"type": "float"},
					"high_price": {"type": "float"},
					"last_price": {"type": "float"},
					"low_price": {"type": "float"},
					"sell_price": {"type": "float"},
					"unit_amount": {"type": "float"},
					"volume": {"type": "float"},
					"contract_type": {"type": "string"},	
					"contract_id": {"type": "string", "index": "no"}, 
					"currency_pair": {"type": "string"}				
				}
			}
		} 
		return self.futureMapping

	def getKline(self, dataSet, channelName): 
		futureDto = {}
		uniqueId = uuid.uuid4()
		futureDto["uuid"] = str(uniqueId) 
		recordDate = datetime.datetime.now(TIMEZONE)
		futureDto["date"] = recordDate

		if type(dataSet) is list: 
			try: 
				# [time, open_price, highest_price, lowest_price, close_price, volume]
				futureRegex = re.search("ok_sub_spotusd_(b|l)tc_kline_(.+)", channelName)
				futureType = futureRegex.group(2)
				currencySymbol = futureRegex.group(1)
				futureDto["timestamp"] = str(dataSet[0])
				futureDto["open_price"] = float(dataSet[1])
				futureDto["highest_price"] = float(dataSet[2])
				futureDto["lowest_price"] = float(dataSet[3])
				futureDto["close_price"] = float(dataSet[4])

				theVol = str(dataSet[5])
				theVol = theVol.replace(",", "")
				theVolFloat = float(theVol)

				futureDto["volume"] = float(theVolFloat)
				pair = currencySymbol.upper() + "TCUSD"
				futureDto["currency_symbol"] = str(pair)
				futureDto["contract_type"] = str(futureType)
			except: 
				pass
		else: 
			print (dataSet)
			try: 
				for jsonData in dataSet: 
					futureData = jsonData["data"] 

					# {'vol': '696068.00', 'high': 471.64, 'contractId': '20160325012', 'low': 461.74, 'buy': 464.68, 'last': '464.68', 'hold_amount': 235764, 'unitAmount': 100, 'sell': 464.76}

					futureDto["volume"] = float(futureData["vol"])
					futureDto["high"] = float(futureData["high"]) 
					futureDto["contract_id"] = str(futureData["contractId"]) 
					futureDto["low"] = float(futureData["low"]) 
					futureDto["buy"] = float(futureData["buy"]) 
					futureDto["last"] = float(futureData["last"]) 
					futureDto["hold_amount"] = float(futureData["hold_amount"]) 
					futureDto["unit_amount"] = float(futureData["unitAmount"]) 
					futureDto["sell"] = float(futureData["sell"]) 
					futureDto["currency_symbol"] = str(currencySymbol)
			except: 
				pass 
		return futureDto 

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
			try: 
				curChannel = dataSet["channel"]
				print (curChannel)
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
				elif curChannel in self.klineChannels: 
					print ("WE IN DA KLINE LIST!")
					theData = dataSet["data"]
					dto = self.getKline(theData, curChannel)
					self.postDto(dto, "live_crypto_candlesticks")
				elif curChannel in self.futureChannels: 
					print ("WE ARE IN A FUTURE CHANNEL!")
					theData = dataSet["data"]
					dto = self.getFutureTickerMappingDto(theData, curChannel) 
					print ("CRYPTO FUTURES!!!!")
					print(dto)
					self.postDto(dto, "live_crypto_futures_contracts")

			except:
				pass
		pass

	def getFutureTickerMappingDto(self, data, channelName): 
		print("\n\n\n\n\nOOOOOO\n\n---\n\n")
		futureDto = {}
		uniqueId = uuid.uuid4()
		futureDto["uuid"] = str(uniqueId) 
		recordDate = datetime.datetime.now(TIMEZONE)
		futureRegex = re.search("ok_sub_futureusd_(b|l)tc_ticker_(.+)", channelName)
		futureType = futureRegex.group(2)
		currencySymbol = futureRegex.group(1)
		futureDto["date"] = recordDate
		futureDto["buy_price"] = float(data["buy"])
		futureDto["contract_id"] = str(data["contractId"])
		futureDto["high_price"] = float(data["high"])
		futureDto["last_price"] = float(data["last"])
		futureDto["low_price"] = float(data["low"])
		futureDto["sell_price"] = float(data["sell"])
		futureDto["unit_amount"] = float(data["unitAmount"])
		futureDto["volume"] = float(data["vol"])

		# HACKY AS FUCK but im tired so fuck off
		currencyPairType = currencySymbol.upper() + "TCUSD"
		futureDto["currency_pair"] = str(currencyPairType)
		futureDto["contract_type"] = str(futureType)
		return futureDto
		

	def getJsonData(self, okcoinData): 
		tempData = okcoinData
		dataStr = tempData.decode(encoding='UTF-8')
		jsonData =json.loads(dataStr)
		return jsonData

