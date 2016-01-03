# #!/usr/bin/python3
# __author__ = "donnydevito"
# __copyright__   = "Copyright 2015, donnydevito"
# __license__ = "MIT"

# import websockets, time, sys, json, hashlib, zlib, base64, argparse, asyncio
# from ws4py.client.threadedclient import WebSocketClient

import websocket, time, datetime, sys, json, hashlib, zlib, base64, json, re, elasticsearch, argparse, uuid, pytz

BITFINEX_WEBSOCKET_URL = "wss://api2.bitfinex.com:3000/ws"
OKCOIN_WEBSOCKET_URL = "wss://real.okcoin.com:10440/websocket/okcoinapi"

# UTC ALL THE TIME, FOREVER AND EVER. 
TIMEZONE = pytz.timezone('UTC')

# ***** CHANGE THIS TO BE THE URL OF YOUR ELASTICSEARCH SERVER *****
ELASTICSEARCH_HOST = "http://localhost:9200"

# # Bitfinex API time limit in seconds 
# API_TIME_LIMIT = 1

# Default index name in elasticsearch to use for the btc_usd market data aggregation
DEFAULT_INDEX_NAME = "btcwebsockettickerarchive"


es = None


## TODO: RESTRUCTURE THIS INTO MORE MEANINGFUL CLASSES

def createMappings(es): 
	mappingCreated = False
	try: 
		bitfinexMapping = {
			"bitfinex": {
				"properties": {
					"uuid": { "type": "string", "index": "no"}, 
					"date": {"type": "date"},
					"last_price": {"type": "float"},
					"timestamp": {"type": "string", "index": "no"},
					"volume": {"type": "float"},
					"mid": {"type": "float"},
					"high": {"type": "float"},
					"ask": {"type": "float"},
					"low": {"type": "float"},
					"bid": {"type": "float"}
				}
			}
		}
		okcoinMapping = { 
			"okcoin": { 
				"properties": {
					"uuid": { "type": "string", "index": "no"}, 
					"date": {"type":"date"}, 
					"last_price": {"type": "float"}, 
					"timestamp": {"type": "string", "index": "no"},
					"volume": {"type": "float"},
					"high": {"type": "float"},
					"ask": {"type": "float"},
					"low": {"type": "float"},
					"bid": {"type": "float"}
				}
			}
		}
		es.indices.create(DEFAULT_INDEX_NAME)
		es.indices.put_mapping(index=DEFAULT_INDEX_NAME, doc_type="bitfinex", body=bitfinexMapping)
		es.indices.put_mapping(index=DEFAULT_INDEX_NAME, doc_type="okcoin", body=okcoinMapping)
		mappingCreated = True
	except: 
		pass
	return mappingCreated

def on_open(self):
    self.send("{'event':'addChannel','channel':'ok_btcusd_ticker','binary':'true'}")

def on_message(self, event):
    okcoinData = inflate(event) #data decompress
    doit(okcoinData)

def doit(data): 
	print("*********~*~**~*~*******")
	tempData = data
	dataStr = tempData.decode(encoding='UTF-8')


	jsonData =json.loads(dataStr)
	for item in jsonData: 
		dataItem = item["data"]
		processTickerData(dataItem)

	uniqueIdentifier = uuid.uuid4()
	# for dataPoint in jsonData: 
	# 	okcoinDto = {}
	# 	dataJson = dataPoint["data"]
	# 	okCoinTimestamp = dataJson["timestamp"] 
	# 	# print(okCoinTimestamp)
	# 	# dateRecv = datetime.datetime.fromtimestamp(float(okCoinTimestamp))
	# 	# print(dateRecv)
	# 	okcoinDto = {}
	# 	okcoinDto["uuid"] = uniqueIdentifier
	# 	# okcoinDto["date"] = dateRecv
	# 	okCoinDto["timestamp"] = str(okCoinTimestamp)
	# 	okCoinDto["last_price"] = float(dataJson["last"])
	# 	okCoinDto["volume"] = float(dataJson["vol"]) 
	# 	okCoinDto["high"] = float(dataJson["high"])
	# 	okCoinDto["ask"] = float(dataJson["sell"]) 
	# 	okCoinDto["low"] = float(dataJson["low"]) 
	# 	okCoinDto["bid"] = float(dataJson["buy"])
	# 	print("\t---")
	# 	print(okcoinDto)
	# 	print("\t---")

	print("\t---")
	print("\t---\n")
	print("*********~*~**~*~*******")
	print(data)

def processTickerData(item): 
	print(len(item))

	itemDict = dict(item)

	okCoinDto = {}

	okCoinTimestamp = itemDict["timestamp"]
	uniqueId = uuid.uuid4()

	dateRecv = datetime.datetime.fromtimestamp((float(okCoinTimestamp) / 1000), TIMEZONE)

	volume = itemDict["vol"]
	volume = volume.replace(",", "") 

	lastPrice = itemDict["last"]
	# lastPrice = lastPrice.replace(",", "")

	highPrice = itemDict["high"]
	# highPrice = highPrice.replace(",", "") 
	
	askPrice = itemDict["sell"]
	# askPrice = askPrice.replace(",", "") 

	lowPrice = itemDict["low"]
	# lowPrice = lowPrice.replace(",", "")

	bidPrice = itemDict["buy"]
	# bidPrice = bidPrice.replace(",", "")

	okCoinDto["uuid"] = str(uniqueId)
	okCoinDto["date"] = dateRecv
	okCoinDto["timestamp"] = str(okCoinTimestamp)
	okCoinDto["last_price"] = float(lastPrice)
	okCoinDto["volume"] = float(volume) 
	okCoinDto["high"] = float(highPrice) 
	okCoinDto["ask"] = float(askPrice)
	okCoinDto["low"] = float(lowPrice)
	okCoinDto["bid"] = float(bidPrice)
	putNewDocumentRequest = es.create(index=DEFAULT_INDEX_NAME, doc_type='okcoin', ignore=[400], id=uniqueId, body=okCoinDto)
	successful = putNewDocumentRequest["created"]
	
	if successful == True: 
		print("WEBSOCKET ENTRY ADDED TO ES CLUSTER")
	else: 
		print("!! FATAL !!: WEBSOCKET ENTRY NOT ADDED TO ES CLUSTER")


	print(okCoinDto)

	return item

def inflate(okcoinData):
    decompressedData = zlib.decompressobj(
            -zlib.MAX_WBITS 
    )
    inflatedData = decompressedData.decompress(okcoinData)
    inflatedData += decompressedData.flush()
    return inflatedData

def on_error(self, event):
    print (event)

def on_close(self, event):
    print (event)

if __name__ == "__main__":
	es = elasticsearch.Elasticsearch([ELASTICSEARCH_HOST])
	createMappings(es)
	websocket.enableTrace(False)
	ws = websocket.WebSocketApp(OKCOIN_WEBSOCKET_URL, on_message = on_message, on_error = on_error, on_close = on_close)
	ws.on_open = on_open
	ws.run_forever()



# def getArgs(): 
# 	parser = argparse.ArgumentParser(description='BTC elastic search websockets data collector')
# 	parser.add_argument('--okcoin_secret', default="zzzzzzz")
# 	parser.add_argument('--okcoin_key', default="zzzzzzzz")
# 	args = parser.parse_args()
# 	return args

# def buildMySign(params, secretKey):
#     sign = ''
#     for key in sorted(params.keys()):
#         sign += key + '=' + str(params[key]) + '&'
#     return  hashlib.md5((sign+'secret_key='+secretKey).encode("utf-8")).hexdigest().upper()



# class DummyClient(WebSocketClient):
# 	def opened(self):
# 	    print("Connection opened")
# 	    self.send("{'event':'addChannel','channel':'ok_btcusd_ticker'")
# 	    yield self.recv()
# 	def closed(self, code, reason=None):
# 		print("Closed down", code, reason)

# 	def received_message(self, m):
# 		print(str(m))
# 		# if len(m) == 175:
# 		# 	self.close(reason='Bye bye')

# # def okcoinOnMessage(self, event): 
# # 	okcoinData = inflate(event)
# # 	print(okcoinData)

# # def okcoinOnError(self, event): 
# # 	print(event) 

# # def okcoinOnClose(self, event): 
# # 	logTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# # 	print("[" + logTime + "]: DISCONNECTING FROM OKCOIN WEBSOCKET")

# # def okcoinOnOpen(self, event): 
# #     self.send("{'event':'addChannel','channel':'ok_btcusd_ticker'}")


# # class OkcoinWebSocket(WebSocket):

# #     def handleMessage(self):
# #        for client in clients:
# #           if client != self:
# #              client.sendMessage(self.address[0] + u' - ' + self.data)

# #     def handleConnected(self):
# #        print self.address, 'connected'
# #        for client in clients:
# #           client.sendMessage(self.address[0] + u' - connected')
# #        clients.append(self)

# #     def handleClose(self):
# #        clients.remove(self)
# #        print self.address, 'closed'
# #        for client in clients:
# #           client.sendMessage(self.address[0] + u' - disconnected')


# async def okCoinConnectToSocket():
# 	print("O HAI")
# 	async with websockets.connect(OKCOIN_WEBSOCKET_URL) as websocket:
# 		print("the lulz")
# 		# handshake = websocket.handshake.build_request({'event':'addChannel','channel':'ok_btcusd_ticker'})
# 		print(websocket.send("{'event':'addChannel','channel':'ok_btcusd_ticker'"))
# 		websocket.ping()
# 		websocket.pong()
# 		print(str(websocket.recv()))

# if __name__ == "__main__": 
	
# 	# asyncio.get_event_loop().run_until_complete(okCoinConnectToSocket())
#     try:
#         ws = DummyClient(OKCOIN_WEBSOCKET_URL)
#         ws.connect()
#         ws.run_forever()
#     except KeyboardInterrupt:
#         ws.close()
# 	# args = getArgs()
# 	# if args.okcoin_secret != None and args.okcoin_key != None: 
# 	# 	print("Credentials set for Okcoin...")
# 	# else: 
# 	# 	raise IOError("Please enter all credentials for okcoin (see -h for help)") 
# 	# okcoinApiKey = args.okcoin_key
# 	# okcoinApiSecret = args.okcoin_secret

# 	# websockets.enableTrace(False) 
# 	# okcoinWebSocket = websockets.WebSocketApp(OKCOIN_WEBSOCKET_URL, on_message = okcoinOnMessage, on_error = okcoinOnError, on_close = okcoinOnClose) 
# 	# okcoinWebSocket.on_open = okcoinOnOpen
# 	# okcoinWebSocket.run_forever()

