#!/usr/bin/python3
__author__ = "donnydevito"
__copyright__   = "Copyright 2015, donnydevito"
__license__ = "MIT"

import elasticsearch
def createMappings(es, indexName): 
	mappingCreated = False
	try: 
		bitfinexTickerMapping = {
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
		okcoinTickerMapping = { 
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
					"price": { "type": "float"},
					"amount": {"type": "float"}, 
					"count": {"type": "float"}, 
					"order_type": {"type": "string"} 
					# "orders" : { 
		   #              "type" : "nested",
		   #              "properties": { 

		   #              }
					# }
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
					"price": { "type": "float"},
					"amount": {"type": "float"}, 
					"order_type" : { "type": "string"} 
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
					"uuid": { "type": "string", "index": "no" }, 
					"date" : { "type": "date" }, 
					"sequenceId": { "type" : "string", "index":"not_analyzed"}, 
					"tradeId" : { "type" : "string", "index":"not_analyzed"}, 
					"timestamp": {"type": "string", "index": "no"},
					"price": {"type": "float"}, 
					"amount": {"type": "float"},
					"order_type" : { "type": "string"} 
				 }
			}
		} 

		okCoinCompletedTradeMapping = { 
			"ok_coin_completed_trade": {
				"properties": { 
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

		okcoinCandleStickMapping = { 
			"ok_coin_candlestick": {  
				"properties": { 
					"uuid": { "type": "string", "index": "no" }, 
					"date" : { "type": "date" }, 
					"candle_type" : { "type": "string"}, 
					"timestamp": {"type": "string", "index": "no"},
					"open_price": { "type" : "float" }, 
					"highest_price": { "type" : "float" }, 
					"lowest_price": { "type" : "float" }, 
					"close_price": { "type" : "float" }, 
					"volume": { "type" : "float" }
				}
			}
		}

		okcoinFutureTickerMapping = { 
			"ok_btcusd_future_ticker": { 
				"properties": { 
					"uuid": { "type": "string", "index": "no" }, 
					"date" : { "type": "date" }, 
					"contract_type" : { "type": "string"}, 
					"contract_id" : { "type": "string", "index": "not_analyzed"}, 
					"buy" : {"type": "float"}, 
					"high" : {"type": "float"}, 
					"last" : {"type": "float"}, 
					"low" : {"type": "float"}, 
					"sell" : {"type": "float"}, 
					"hold_amount": {"type": "float"}, 
					"unit_amount" : {"type": "float"}, 
					"volume" : {"type": "float"}
				} 
			} 
		}
		okcoinFutureIndexMapping = { 
			"ok_btcusd_future_index": { 
				"properties": { 
					"uuid": { "type": "string", "index": "no" }, 
					"date" : { "type": "date" }, 	
					"timestamp" : {"type": "string", "index": "no"}, 
					"price": { "type": "float"}	
				}
			} 
		}
		tickerIndex = "btc_tickers"
		orderBookIndex = "btc_orderbooks_live"
		completedTradesIndex = "btc_completed_trades"
		futuresIndex = "btc_futures"
		candlestickIndex = "btc_candlesticks"
		# createIndex(es, tickerIndex)
		createIndex(es, orderBookIndex) 
		# createIndex(es, completedTradesIndex)
		# createIndex(es, futuresIndex)
		# createIndex(es, candlestickIndex)
		# es.indices.put_mapping(index=tickerIndex, doc_type="bitfinex_ticker", body=bitfinexTickerMapping)
		# es.indices.put_mapping(index=tickerIndex, doc_type="okcoin_ticker", body=okcoinTickerMapping)
		es.indices.put_mapping(index=orderBookIndex, doc_type="bitfinex_order_book", body=bitfinexOrderBookMapping)
		es.indices.put_mapping(index=orderBookIndex, doc_type="okcoin_order_book", body=okcoinOrderBookMapping)
		# es.indices.put_mapping(index=completedTradesIndex, doc_type="bitfinex_completed_trade", body=bitfinexCompletedTradeMapping)
		# es.indices.put_mapping(index=completedTradesIndex, doc_type="ok_coin_completed_trade", body=okCoinCompletedTradeMapping)
		# es.indices.put_mapping(index=futuresIndex, doc_type="ok_btcusd_future_ticker", body=okcoinFutureTickerMapping)
		# es.indices.put_mapping(index=candlestickIndex, doc_type="ok_coin_candlestick", body=okcoinCandleStickMapping)
		# es.indices.put_mapping(index=futuresIndex, doc_type="ok_btcusd_future_index", body=okcoinFutureIndexMapping)
		mappingCreated = True
	except: 
		raise
	return mappingCreated

def createIndex(es, name): 
	try: 
		es.indices.create(name)
	except: 
		pass
