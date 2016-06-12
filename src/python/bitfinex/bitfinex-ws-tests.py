#!/usr/bin/python3
# Author: Joseph Bull ("Currentsea") 

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
	


