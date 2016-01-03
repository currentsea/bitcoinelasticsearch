#!/usr/bin/python3
__author__ = "donnydevito"
__copyright__   = "Copyright 2015, donnydevito"
__license__ = "MIT"

import websockets, time, sys, json, hashlib, zlib, base64


BITFINEX_WEBSOCKET_URL = "wss://api2.bitfinex.com:3000/ws"
OKCOIN_WEBSOCKET_URL = "wss://real.okcoin.com:10440/websocket/okcoinapi"
OKCOIN_API_KEY = ""
OKCOIN_API_SECRET = ""



def getArgs(): 
	parser = argparse.ArgumentParser(description='BTC elastic search websockets data collector')
	parser.add_argument('--okcoin_secret', default="zzzzzzz")
	parser.add_argument('--okcoin_key', default="zzzzzzzz")
	args = parser.parse_args()
	return args

def buildMySign(params,secretKey):
    sign = ''
    for key in sorted(params.keys()):
        sign += key + '=' + str(params[key]) +'&'
    return  hashlib.md5((sign+'secret_key='+secretKey).encode("utf-8")).hexdigest().upper()

if __name__ == "__main__": 
	args = getArgs()
	if args.okcoin_secret != None and args.okcoin_key != None: 
		print("Credentials set for Okcoin...")
	else: 
		raise IOError("Please enter all credentials for okcoin (see -h for help)") 
	okcoinApiKey = args.okcoin_key
	okcoinApiSecret = args.okcoin_secret