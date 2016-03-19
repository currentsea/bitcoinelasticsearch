import threading, simplejson 

import tornado.httpserver
import tornado.ioloop
import tornado.web
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


  class EpiVizPy(object):
      def __init__(self, server_path=r'/ws'):
          self._thread = None
          self._server = None
          self._application = tornado.web.Application([(server_path, EpiVizPyEndpoint)])

      def start(self, port=8888):
          self.stop()
          self._thread = threading.Thread(target=lambda: self._listen(port)).start()

      def stop(self):
          if self._server != None:
              tornado.ioloop.IOLoop.instance().stop()
              self._server = None
              self._thread = None

      def _listen(self, port):
          self._server = tornado.httpserver.HTTPServer(self._application)
          self._server.listen(port)
          tornado.ioloop.IOLoop.instance().start()
    