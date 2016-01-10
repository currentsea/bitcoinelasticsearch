#!/usr/bin/python3
__author__ = "donnydevito"
__copyright__   = "Copyright 2015, donnydevito"
__license__ = "MIT"

def createMappings(es, indexName): 
	mappingCreated = False
	try: 
		bitfinexMapping = {
			"bitfinex_ticker": {
				"properties": {
					"uuid": { "type": "string", "index": "no"}, 
					"date": {"type": "date"},
					"last_price": {"type": "float"},
					"timestamp": {"type": "string", "index": "no"},
					"volume": {"type": "float"},
					"high": {"type": "float"},
					"ask": {"type": "float"},
					"low": {"type": "float"},
					"dailyChange": {"type": "float"}, 
					"dailyDelta": {"type" : "float"}, 
					"askVolume": {"type": "float"}, 
					"bidVolume": {"type": "float"},
					"bid": {"type": "float"}
				}
			}
		}
		okcoinMapping = { 
			"okcoin_ticker": { 
				"properties": {
					"uuid": { "type": "string", "index": "no"}, 
					"date": {"type":"date"}, 
					"last_price": {"type": "float"}, 
					"timestamp": {"type": "string", "index": "no"},
					"volume": {"type": "float"},
					"high": {"type": "float"},
					"ask": {"type": "float"},
					"low": {"type": "float"},
					"bid": {"type": "float"}
				}
			}
		}
		bitfinexOrderBookMapping = { 
			"bitfinex_order_book": { 
				"properties": { 
					"uuid": { "type": "string", "index": "no"}, 
					"date": {"type":"date"}, 
					"orders" : { 
		                "type" : "nested",
		                "properties": { 
							"price": { "type": "float"},
							"amount": {"type": "float"}, 
							"count": {"type": "float"}, 
							"order_type": {"type": "string"} 
		                }
					}
					# "largest_bid_order_weighted_by_volume"
					# "largest_ask_order_weighted_by_volume"
					# "largest_order_by_volume" 
					# "standard_deviation_orders"
					# "new_order_delta"

				}

			}
		} 
		okcoinOrderBookMapping = { 
			"okcoin_order_book": { 
				"properties": { 
					"uuid": { "type": "string", "index": "no"}, 
					"date": {"type":"date"}, 
					"orders" : { 
		                "type" : "nested",
		                "properties": { 
							"price": { "type": "float"},
							"amount": {"type": "float"}, 
							"order_type" : { "type": "string"} 
		                }
					}
					# "largest_bid_order_weighted_by_volume"
					# "largest_ask_order_weighted_by_volume"
					# "largest_order_by_volume" 
					# "standard_deviation_orders"
					# "new_order_delta"

				}

			}
		} 
		bitfinexCompletedTradeMapping = { 
			"bitfinex_completed_trade": { 
				"properties": { 

					# 		SEQ	string	Trade sequence id
					# TIMESTAMP	int	Unix timestamp of the trade.
					# PRICE	float	Price at which the trade was executed
					# AMOUNT	float	How much was bought (positive) or sold (negative).
					# The order that causes the trade determines if it is a buy or a sell.
					"uuid": { "type": "string", "index": "no" }, 
					"date" : { "type": "date" }, 
					"tradeId" : { "type" : "string", "index":"not_analyzed"}, 
					"timestamp": {"type": "string", "index": "no"},
					"price": {"type": "float"}, 
					"amount": {"type": "float"},
					"order_type" : { "type": "string"} 
				 }
			}
		} 
		okCoinFutureThisWeekMapping = { 
			"ok_coin_futures_this_week": {
				"properties": { 
					"uuid": { "type" : "string", "index": "no" }, 
					"date": { "type" : "date" }, 
					"buy" :  { "type" : "float" }, 
					"high": { "type" : "float" }, 
					"low": { "type" : "float" }, 
					"last": { "type" : "float" }, 
					"sell": { "type" : "float" }, 
					"amount": { "type" : "float" }, 
					"volume": { "type" : "float" }, 
					"contractId": { "type" : "string", "index": "not_analyzed" }
				}
			}
		}

		es.indices.create(indexName)
		es.indices.put_mapping(index=indexName, doc_type="bitfinex_ticker", body=bitfinexMapping)
		es.indices.put_mapping(index=indexName, doc_type="okcoin_ticker", body=okcoinMapping)
		es.indices.put_mapping(index=indexName, doc_type="bitfinex_order_book", body=bitfinexOrderBookMapping)
		es.indices.put_mapping(index=indexName, doc_type="okcoin_order_book", body=okcoinOrderBookMapping)
		es.indices.put_mapping(index=indexName, doc_type="bitfinex_completed_trade", body=bitfinexCompletedTradeMapping)
		es.indices.put_mapping(index=indexName, doc_type="ok_coin_futures_this_week", body=okCoinFutureThisWeekMapping)

		mappingCreated = True
	except: 
		pass
	return mappingCreated
