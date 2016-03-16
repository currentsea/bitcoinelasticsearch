#!/usr/bin/python3
# Author: Joseph Bull ("Currentsea") 
# Do not redistribute without permission 
__author__ = "currentsea"
__copyright__   = "Copyright 2016, currentsea"

import json, uuid, datetime, pytz, elasticsearch, argparse
import uuid
import datetime
import pytz
import elasticsearch
import argparse

from websocket import create_connection
from create_mappings import createMappings

DEFAULT_WEBSOCKETS_URL = "wss://api2.bitfinex.com:3000/ws"
DEFAULT_ELASTICSEARCH_URL = "http://localhost:9200"

class Bitfinex(): 
	def __init__(self, wsUrl=DEFAULT_WEBSOCKETS_URL, esUrl=DEFAULT_ELASTICSEARCH_URL): 
		self.wsUrl = wsUrl
		self.esUrl = esUrl

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
