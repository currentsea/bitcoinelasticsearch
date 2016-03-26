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
import json
import requests
import bitfinex_properties
from websocket import create_connection


class BitfinexPrivate: 
	def __init__(self, apiKey=bitfinex_properties.BITFINEX_API_KEY, apiSecret=bitfinex_properties.BITFINEX_API_SECRET, wsUrl=bitfinex_properties.WEBSOCKET_URL, apiUrl=bitfinex_properties.REST_API_URL): 
		self.apiKey = apiKey
		self.apiSecret = apiSecret
		self.wsUrl = wsUrl
		self.apiUrl = apiUrl
		self.connectWebsocket()
		self.symbols = self.getSymbols()
		self.channelMappings = self.getChannelMappings()

	def connectWebsocket(self):
		try:
			self.ws = create_connection(self.wsUrl)
		except:
			raise
		return True

	# def subscribeList(self, wsDataList): 
	# 	counter = 0
	# 	for item in wsData: 
	# 		try: 
	# 			self.ws.send(json.dumps(item))
	# 		except: 
	# 			pass
	# def subscribe(self, wsData): 
	# 	if type(wsData) is list: 
			
	# 	else: 
	# 		try: 
	# 			self.ws.send(json.dumps(wsData))
	# 		except: 
	# 			pass

	def getSymbols(self):
		symbolsApiEndpoint = self.apiUrl + "/symbols"
		print ("SYMBOLS ENDPOINT: " + symbolsApiEndpoint)
		try:
			req = requests.get(symbolsApiEndpoint)
			reqJson = req.json()
		except:
			raise
		return reqJson

	def subscribeAllChannels(self): 
		for symbol in self.symbols:
			self.ws.send(json.dumps({"event": "subscribe", "channel": "book", "pair": symbol, "prec": "P0",  "len":"100"}))
			self.ws.send(json.dumps({"event": "subscribe", "channel": "ticker", "pair": symbol}))
			self.ws.send(json.dumps({"event": "subscribe", "channel": "trades", "pair": symbol}))

	def getChannelMappings(self):
		allChannelsSubscribed = False
		channelDict = {}
		channelMappings = {}
		self.subscribeAllChannels()

		while (allChannelsSubscribed == False):
			resultData = self.ws.recv()
			try:
				dataJson = json.loads(resultData)
					# print (dataJson)
				pairName = str(dataJson["pair"])
				pairChannelType = str(dataJson["channel"])
				identifier = pairName
				channelId = dataJson["chanId"]
				channelDict[channelId] = identifier
				channelMappings[channelId] = dataJson
				print ("SUBSCRIBE TO CHANNEL " + str(channelId) + " WITH PAIR NAME: " + pairName)
				symbolLength = len(self.symbols)
				# CHANNELS ARE ALL SUBSCRIBED WHEN SYMBOL LENGTH * # # # # # # # # 
				targetLength = symbolLength * 3 
				# IF THIS SAVED YOU HOURS OF DEBUGGING, YOU'RE FUCKING WELCOME * #
				if (len(channelDict) == targetLength):
					allChannelsSubscribed = True
					print ("all channels subscribed..")
			except TypeError:
				pass
			except KeyError:
				print resultData
				pass
		return channelMappings


if __name__ == "__main__": 
	BitfinexPrivate()