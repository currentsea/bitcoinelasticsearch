# #!/usr/bin/python3
# __author__ = "donnydevito"
# __copyright__   = "Copyright 2015, donnydevito"
# __license__ = "MIT"

# import websockets, time, sys, json, hashlib, zlib, base64, argparse, asyncio
# from ws4py.client.threadedclient import WebSocketClient

import websocket, time, sys, json, hashlib, zlib, base64, json, re, elasticsearch, argparse

# BITFINEX_WEBSOCKET_URL = "wss://api2.bitfinex.com:3000/ws"
OKCOIN_WEBSOCKET_URL = "wss://real.okcoin.com:10440/websocket/okcoinapi"
# OKCOIN_API_KEY = ""
# OKCOIN_API_SECRET = ""

def on_open(self):
    self.send("{'event':'addChannel','channel':'ok_btcusd_ticker','binary':'true'}")

def on_message(self, event):
    okcoinData = inflate(event) #data decompress
    doit(okcoinData)

def doit(data): 
	print(data)

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
    print ('DISCONNECTED FROM OKCOIN FEED')

if __name__ == "__main__":
    
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

