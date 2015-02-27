import sys
sys.path.append('gen-py.tornado')
from tornado import ioloop, gen
io_loop = ioloop.IOLoop.instance()
from scrapper import Scrapper
from thrift.protocol import TJSONProtocol
from thrift_amqp_tornado import TAMQPTornadoTransport


def this_joint():
    communicate(callback=io_loop.stop)


@gen.engine
def resolve(i, f, callback):
    yield f
    print i
    callback()


@gen.engine
def communicate(callback):
    transport = TAMQPTornadoTransport()
    pfactory = TJSONProtocol.TJSONProtocolFactory()
    client = Scrapper.Client(transport, pfactory)

    yield gen.Task(transport.open)

    futures = []

    for i in xrange(100):
        f = client.scrape('http://google.com/')
        futures.append(f)

    i = 0

    for f in futures:
        i += 1
        yield gen.Task(resolve, i, f)

    client._transport.close()

    callback()

io_loop.add_callback(this_joint)
io_loop.start()
