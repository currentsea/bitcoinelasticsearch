import simplejson
import tornado.websocket

class EpiVizPyEndpoint(tornado.websocket.WebSocketHandler):

  def __init__(self, *args, **kwargs):
      super(EpiVizPyEndpoint, self).__init__(*args, **kwargs)

  def open(self):
      print ('new connection')

  def on_message(self, json_message):
      print ('message received ')
      print(json_message)
      message = simplejson.loads(json_message)

  def on_close(self):
      print ('closed connection') 
      