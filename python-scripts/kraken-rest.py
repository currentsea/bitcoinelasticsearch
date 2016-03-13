import requests, json, argparse

from time import sleep 
#  asks = ask side array of array entries(<price>, <volume>, <timestamp>)
#  bids = bid side array of array entries(<price>, <volume>, <timestamp>)

def getArgs(): 
	parser = argparse.ArgumentParser(description='BTC elastic search data collector')
	parser.add_argument('--host')
	parser.add_argument('--currency')
	parser.add_argument('--forever', action='store_true', default=False)
	parser.add_argument('--max_records', action='store_true', default=3600)

	# TODO: add more params here

	args = parser.parse_args()
	return args

def getOrderbook(): 
	req = requests.get("")

if __name__ == "__main__": 
	args = getArgs()