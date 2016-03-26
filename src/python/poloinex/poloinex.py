#!/usr/bin/python
# Program: poloinex.py 
# Description: Wrapper around the poloinex exchange 
# Author: Joseph Bull 
# Copyright 2016 
# Do Not Redistribute 
# Long Live Bitcoin! 

# Intened for use with python 2.7 or higher (not python3 compatible)

# import argparse
# import hmac
# import hashlib
# import time
# import json
# import urllib
# import urllib2
# import requests
# import pytz 
# import elasticsearch
# import poloinex
# import uuid
# import datetime
import sys
import elasticsearch
from time import sleep
# from twisted.python import log
# from twisted.internet import reactor
# from websocket import create_connection
# from poloinex_websocket import PoloinexWebsocketProtocol
# from autobahn.twisted.websocket import WebSocketClientFactory
from os import environ
from autobahn.asyncio.wamp import ApplicationSession
from autobahn.asyncio.wamp import ApplicationRunner
from asyncio import coroutine

# from time import sleep
ELASTICSEARCH_HOST_URL = "https://search-bitcoins-2sfk7jzreyq3cfjwvia2mj7d4m.us-west-2.es.amazonaws.com" 
WEBSOCKETS_API_URL = "wss://api.poloniex.com:443"
WEBSOCKETS_API_PORT = 443


class PoloinexWebsocketComponent(ApplicationSession): 
	
	def __init__(self, realm):
		self.realm = realm
	
	def onConnect(self):
		self.join(self.realm)
		self.received = 0
		sub = yield self.subscribe(self.onTicker, WEBSOCKETS_API_URL, "realm1")


	def onTicker(self, *args):
		print("Ticker event received:", args)

		try:
			yield from self.subscribe(onTicker, 'ticker')
		except Exception as e:
			print("Could not subscribe to topic:", e)

class Poloinex: 
	def __init__(self, apiKey, apiSecret, esUrl=ELASTICSEARCH_HOST_URL, wsUrl=WEBSOCKETS_API_URL, wsPort=WEBSOCKETS_API_PORT): 
		self.apiKey = apiKey
		self.apiSecret = apiSecret
		self.wsUrl = wsUrl
		self.wsPort = wsPort
		self.connectWebsocket()
		self.connectElasticsearch()

	def connectWebsocket(self):
		try:
			# socketOptions = {}
			# socketOptions["realm"] = "realm1" 
			# self.ws = create_connection(self.wsUrl, headers=socketOptions)
		   log.startLogging(sys.stdout)
		   self.wsFactory = WebSocketClientFactory()
		   self.wsFactory.protocol = PoloinexWebsocketProtocol
		   reactor.connectTCP(self.wsUrl, self.wsPort, self.wsFactory)
		   reactor.run()
		except:
			raise
		return True

	def connectElasticsearch(self):
		try:
			self.es = elasticsearch.Elasticsearch([self.esUrl])
		except:
			raise
		return True

if __name__ == "__main__": 
    runner = ApplicationRunner("wss://api.poloniex.com:443", "realm1")
    runner.run(PoloinexWebsocketComponent)

	# poloinex = Poloinex("abcd12345", "abdcde1234", "http://localhost:9200")
	# print (poloinex.apiKey)