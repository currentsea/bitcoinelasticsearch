#!/usr/bin/python3
# Author: Joseph Bull ("***Curren*cy*tsea***") 
# Email: joetbull@gmail.com (alternate: jtbull@uw.edu)
# # # #  Do not redistribute without permission # # # # 
__author__ = "Joseph 'currentsea' Bull"
__copyright__   = "Copyright 2016, seclorum"

import json
import uuid
import pytz
import datetime
import argparse
import requests
import elasticsearch

from websocket import create_connection
from create_mappings import createMappings

DEFAULT_API_URL = "https://api.bitfinex.com/v1"
DEFAULT_WEBSOCKETS_URL = "wss://api2.bitfinex.com:3000/ws"
DEFAULT_ELASTICSEARCH_URL = "http://localhost:9200"

class Bitfinex(): 
	def __init__(self, wsUrl=DEFAULT_WEBSOCKETS_URL, esUrl=DEFAULT_ELASTICSEARCH_URL, apiUrl=DEFAULT_API_URL): 
		self.wsUrl = wsUrl
		self.esUrl = esUrl
		self.apiUrl = apiUrl

	def connectWebsocket(self): 
		try: 
			ws = create_connection(self.wsUrl)
		except: 
			raise 
		return ws

	def connectElasticsearch(self): 
		try: 
			es = elasticsearch.Elasticsearch([self.esUrl])
		except: 
			raise
		return es

	def getSymbols(self): 
		symbolsApiEndpoint = self.apiUrl + "/symbols"
		print ("SYMBOLS ENDPOINT: " + symbolsApiEndpoint) 
		try: 
			req = requests.get(symbolsApiEndpoint)
			reqJson = req.json()
		except: 
			raise
		return reqJson



