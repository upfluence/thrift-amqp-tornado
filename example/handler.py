import sys
sys.path.append('gen-py.tornado')

from thrift.protocol import TJSONProtocol
from scrapper import Scrapper
from thrift_amqp_tornado import TAMQPTornadoServer
from tornado import ioloop, gen
from tornado.httpclient import AsyncHTTPClient
http_client = AsyncHTTPClient()
loop = ioloop.IOLoop.instance()


@gen.engine
def fetch_data(url, callback):
    try:
        r = yield gen.Task(http_client.fetch, url)
        print "done"
        callback(Scrapper.Blog(url=url, content=r.body[:100]))
    except:
        print "Something went wrong"
        callback(Scrapper.Blog())


class ScraperHandler(object):
    def __init__(self):
        pass

    def scrape(self, url):
        return gen.Task(fetch_data, url)

handler = ScraperHandler()
processor = Scrapper.Processor(handler)
pfactory = TJSONProtocol.TJSONProtocolFactory()
server = TAMQPTornadoServer(processor, pfactory)

server.start()
loop.start()
