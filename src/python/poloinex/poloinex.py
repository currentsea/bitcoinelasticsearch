# Author: Joseph Bull 
# Copyright 2016 
# Do Not Redistribute 
# Long Live Bitcoin! 

import argparse
import hmac
import hashlib
import time
import json
import urllib
import urllib2
import requests
import pytz 
import elasticsearch
import poloinex
import uuid
import datetime
from time import sleep
ELASTICSEARCH_HOST = "https://search-bitcoins-2sfk7jzreyq3cfjwvia2mj7d4m.us-west-2.es.amazonaws.com/" 

class Poloinex: 
	def __init__(self, apiKey, apiSecret): 
		self.apiKey = apiKey
		self.apiSecret = apiSecret