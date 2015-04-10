import sys
sys.path.append('gen-py.tornado')
from tornado import ioloop, gen
io_loop = ioloop.IOLoop.instance()
from scrapper import Scrapper
from thrift.protocol import TJSONProtocol
from thrift_amqp_tornado import TAMQPTornadoTransport


@gen.coroutine
def communicate():
    transport = TAMQPTornadoTransport()
    pfactory = TJSONProtocol.TJSONProtocolFactory()
    client = Scrapper.Client(transport, pfactory)

    yield gen.Task(transport.open)

    futures = [client.scrape('http://google.com/') for i in xrange(100)]

    yield futures

    client._transport.close()

    io_loop.stop()
io_loop.add_callback(communicate)
io_loop.start()
