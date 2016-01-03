# Modified version of okcoin_websocket from https://github.com/OKCoin/websocket/blob/master/python/okcoin_websocket.py 
# The library that is being used was released under the LGPL 
# Consider this header notice as such 

import websocket
import time
import sys
import json
import hashlib
import zlib
import base64


def on_open(self):
    self.send("{'event':'addChannel','channel':'ok_btcusd_ticker','binary':'true'}")

def on_message(self, event):
    okcoinData = inflate(event) #data decompress
    print (okcoinData)

def inflate(okcoinData):
    decompressedData = zlib.decompressobj(
            -zlib.MAX_WBITS 
    )
    inflatedData = decompressedData.decompress(okcoinData)
    inflatedData += decompressedData.flush()
    return inflatedData

def on_error(self, event):
    print (event)

def on_close(self, event):
    print ('DISCONNECTED FROM OKCOIN FEED')

if __name__ == "__main__":
    url = "wss://real.okcoin.com:10440/websocket/okcoinapi"
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(host,
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
