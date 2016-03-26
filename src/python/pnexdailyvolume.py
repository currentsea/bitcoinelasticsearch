# Author: Joseph Bull 
# Copyright 2016 
# Long Live Bitcoin! 
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
import argparse, hmac, hashlib, time, json, urllib, urllib2, requests, pytz, elasticsearch, poloinex, uuid, datetime
from time import sleep
ELASTICSEARCH_HOST = "https://search-bitcoins-2sfk7jzreyq3cfjwvia2mj7d4m.us-west-2.es.amazonaws.com/" 

def getArgs(): 
	parser = argparse.ArgumentParser(description='BTC elastic search data collector')
	parser.add_argument('--host', default=ELASTICSEARCH_HOST) 
	parser.add_argument('--forever', action='store_true', default=False)
	parser.add_argument('--max_records', action='store_true', default=3600)
	args = parser.parse_args()
	return args

def getIndeces(elasticHost): 
	pass

def getDtoList(connector): 
	dtoList = []
	tickerData = connector.return24Volume()
	for item in tickerData: 
		dto = {}
		uniqueId = uuid.uuid4()
		dto["uuid"] = str(uniqueId)
		dto["date"] = datetime.datetime.utcnow()
		dto["currency_pair"] = item
		itemDict = tickerData[item]
		# for key in itemDict: 
		# 	dto[key] = itemDict[key]
		# dtoList.append(dto)
		print item
	pass
	#return dtoList

def createIndex(es, name): 
	try: 
		es.indices.get(name)
	except elasticsearch.exceptions.NotFoundError: 
		# print ("Index " + name + " already existed on target host. Continuing...")
		# pass
		print es.indices.create(name)
	except:
		raise

def putMapping(es, indexName, docType): 
	try: 
		pnexTickersMapping = {
			"daily_volume": {
				"properties": {
					"uuid": { "type": "string"},
					"date": {"type": "date"},
					"curency_pair": { "type": "string"},
					"value_pair_left": {"type": "float"}, 
					"value_pair_right": {"type":"float"} 
				} 
			} 
		}  
		es.indices.put_mapping(index=indexName, doc_type=docType, body=pnexTickersMapping)
	except: 
		raise 

def injectData(es, indexName, docType, docBody, conDocs): 	
	putNewDocumentRequest = es.create(index=indexName, doc_type=docType, ignore=[400], id=uuid.uuid4(), body=docBody)
	successful = putNewDocumentRequest["created"]
	if successful == True: 
		print "Added data for " + docType + " (docs consecutively added this run: " + str(conDocs) + ")"
	else: 
		raise

if __name__ == "__main__": 
	args = getArgs()
	indexName = "volume_exchange_data" 
	docType = "poloinex"
	#print args.host
	conDocs = 0
	es = elasticsearch.Elasticsearch(args.host, verify_certs=True) 
	createIndex(es, indexName) 
	putMapping(es, indexName, docType) 
	connector = poloinex.poloniex("", "")
	while 1 == 1: 
		tickerData = getDtoList(connector)
		sleep(0.5)
		for dataPoint in tickerData: 
			conDocs = conDocs + 1
			injectData(es, indexName, docType, dataPoint, conDocs)