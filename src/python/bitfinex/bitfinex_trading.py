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


# from __future__ import absolute_import

import time
import json
import hmac
import base64
import hashlib
import requests
import datetime
import bitfinex_properties
from websocket import create_connection
# from decouple import config

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
		# for symbol in self.symbols:
			# self.ws.send(json.dumps({"event": "subscribe", "channel": "book", "pair": symbol, "prec": "P0",  "len":"100"}))
			# self.ws.send(json.dumps({"event": "subscribe", "channel": "ticker", "pair": symbol}))
			# self.ws.send(json.dumps({"event": "subscribe", "channel": "trades", "pair": symbol}))
			# # payload = {"event": "auth", "apiKey": self.apiKey}
			
			# payload = 
			# payloadBytes = bytes(payload, encoding='utf-8')
			# encodedData = base64.standard_b64encode(payloadBytes)
			theNonce = float(time.time() * 1000000)
			theNonceStr = 'AUTH' + str(theNonce)
			hashDigest = hmac.new(self.apiSecret.encode('utf8'), theNonceStr.encode('utf-8'), hashlib.sha384)
			# encodedData.encode('utf-8') 

			signature = hashDigest.hexdigest()
			reqBody = { 
				"event": "auth", 
				"apiKey": str(self.apiKey), 
				"authSig": str(signature), 
			    "authPayload": str(hashDigest)
			}
			# authJson = json.dumps(reqBody)
			self.ws.send(json.dumps(reqBody))

	def getNonce(self):
		curTime = time.time()
		nonce = str(int(curTime * 1000000))
		authNonce = 'AUTH' + nonce 
		return authNonce

	# def signPayload(self, payload):


		# return {
		# 	"X-BFX-APIKEY": API_KEY,
		# 	"X-BFX-SIGNATURE": signature,
		# 	"X-BFX-PAYLOAD": data
		# }

# var
#     crypto = require('crypto'),
#     api_key = 'API_KEY',
#     api_secret = 'API_SECRET',
#     payload = 'AUTH' + (new Date().getTime()),
#     signature = crypto.createHmac("sha384", api_secret).update(payload).digest('hex');
# w.send(JSON.stringify({
#     event: "auth",
#     apiKey: api_key,
#     authSig: signature,
#     authPayload: payload
# }));
# // request
# {  
#    "event":"auth",
#    "status":"OK",
#    "chanId":0,
#    "userId":"<USER_ID>"
# }



	def getMappingAuthentication(self): 
		payload = 'AUTH' + datetime.datetime.utcnow()

	def getChannelMappings(self):
		allChannelsSubscribed = False
		channelDict = {}
		channelMappings = {}
		self.subscribeAllChannels()
		while (allChannelsSubscribed == False):
			resultData = self.ws.recv()
			print (resultData)
			try:
				dataJson = json.loads(resultData)
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
				targetLength = targetLength + 1
				# targetLength = 4 # fuck it 
				# IF THIS SAVED YOU HOURS OF DEBUGGING, YOU'RE FUCKING WELCOME * #
				if (len(channelDict) == targetLength):
					allChannelsSubscribed = True
			except TypeError:
				pass
			except KeyError: 
				pass
			except: 
				raise
		return channelMappings

if __name__ == "__main__": 
	that = BitfinexPrivate()
#    "event":"auth",
#    "status":"OK",
#    "chanId":0,
#    "userId":"<USER_ID>"
# }