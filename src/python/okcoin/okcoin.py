#!/usr/bin/python3
__author__ = "currentsea"
__copyright__   = "Copyright 2016, currentsea"
__license__ = "MIT"

import pytz

WEBSOCKET_URL = "wss://real.okcoin.com:10440/websocket/okcoinapi"
TIMEZONE = pytz.timezone('UTC')

class Okcoin(): 
	def __init__(self, wsUrl=DEFAULT_WEBSOCKETS_URL, esUrl=DEFAULT_ELASTICSEARCH_URL, apiUrl=DEFAULT_API_URL):
		self.wsUrl = wsUrl
		self.esUrl = esUrl
		self.apiUrl = apiUrl
		# self.symbols = self.getSymbols()
		# self.connectWebsocket()
		# self.connectElasticsearch()
		# self.createIndices()
		# self.getCompletedTradesMapping()
		# self.getOrderbookElasticsearchMapping()
		# self.createMappings()

