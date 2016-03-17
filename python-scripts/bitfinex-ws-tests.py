#!/usr/bin/python3
# Author: Joseph Bull ("Currentsea") 
# Do not redistribute without permission 
__author__ = "currentsea"
__copyright__   = "Copyright 2016, currentsea"

import json, uuid, datetime, pytz, elasticsearch, argparse
from websocket import create_connection
from create_mappings import createMappings
from bitfinex import Bitfinex

DEFAULT_WEBSOCKETS_URL = "wss://api2.bitfinex.com:3000/ws"
DEFAULT_ELASTICSEARCH_URL = "http://localhost:9200"

bitfinexConnector = Bitfinex()

def test_initialization():
	assert bitfinexConnector != None
	assert bitfinexConnector.wsUrl != None
	assert bitfinexConnector.esUrl != None

def test_connect_websocket(): 
	websocketConnection = bitfinexConnector.connectWebsocket()
	assert websocketConnection == True

def test_connect_elasticsearch(): 
	elasticsearchConnection = bitfinexConnector.connectElasticsearch()
	if bitfinexConnector.esUrl != DEFAULT_ELASTICSEARCH_URL: 
		assert elasticsearchConnection == True

def test_get_symbols(): 
	symbolJson = bitfinexConnector.getSymbols()
	assert symbolJson != None
	assert type(symbolJson) is list
	assert len(symbolJson) > 0

def test_connect_orderbook_websocket(): 
	wsConnection = bitfinexConnector.connectOrderbookSocket()
	


