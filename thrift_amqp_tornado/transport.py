from cStringIO import StringIO
import logging
import uuid
import pika

from thrift.TTornado import _Lock
from thrift.transport.TTransport import TTransportBase
import thrift.transport.TTransport

from tornado import gen, ioloop

from pika.adapters import TornadoConnection
from toro import Queue
import constant

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TAMQPTornadoTransport(TTransportBase):
    def __init__(self, channel=None, exchange_name=constant.EXCHANGE_NAME,
                 routing_key=constant.ROUTING_KEY, properties=None,
                 method=None, io_loop=None, **kwargs):
        self._channel = channel
        self._exchange_name = exchange_name
        self._routing_key = routing_key
        self._wbuf = StringIO()
        self._properties = properties
        self._method = method
        self._url = kwargs.get('url', constant.DEFAULT_URL)
        self._reply_to = None
        self._lock = _Lock()
        self._callback = None
        self._connection = None
        self._consumer_tag = None
        self._consumer_name = kwargs.get('consumer_tag')
        self._message_expiration = kwargs.get('message_expiration')
        self._callback_queue = Queue()
        self.io_loop = io_loop or ioloop.IOLoop.instance()

    @gen.coroutine
    def assign_queue(self):
        logger.info("Openning callback queue")
        result = yield gen.Task(self._channel.queue_declare,
                                exclusive=True)
        logger.info("Callback queue openned")
        self._reply_to = result.method.queue
        self._lock.release()
        self._consumer_tag = self._channel.basic_consume(
            self.on_reply_message, self._reply_to,
            consumer_tag=self._consumer_name)

        if self._callback:
            self._callback()

    def on_reply_message(self, _channel, method, properties, body):
        if method:
            self._channel.basic_ack(delivery_tag=method.delivery_tag)
        self._callback_queue.put(body)

    def on_connection_open(self, _connection):
        logger.info("Openning channel")
        self._connection.channel(on_open_callback=self.on_channel_open)

    def open(self, callback=None):
        logger.info("Openning AMQP transport")
        if self._channel is not None:
            logger.info("Already set")
            callback()
        else:
            logger.info("Openning connection")
            self._callback = callback
            self._connection = TornadoConnection(pika.URLParameters(self._url),
                                                 self.on_connection_open,
                                                 self.on_connection_error,
                                                 self.on_connection_error)
            self._lock.acquire()

    @gen.coroutine
    def on_connection_error(self, *args, **kwargs):
        logger.info("Connection failed")
        yield gen.sleep(constant.TIMEOUT_RECONNECT)
        self.start()

    @gen.coroutine
    def readFrame(self):
        result = yield self._callback_queue.get()
        raise gen.Return(result)

    def on_channel_open(self, channel):
        logger.info("Channel openned")
        self._channel = channel
        self.assign_queue()

    def close(self):
        if self._channel:
            if self._consumer_tag:
                self._channel.basic_cancel(consumer_tag=self._consumer_tag,
                                           nowait=True)
            self._channel.close()

    def isOpen(self):
        return self._channem is not None

    def read(self, _):
        assert False, "wrong stuff"

    def write(self, buf):
        self._wbuf.write(buf)

    @gen.coroutine
    def flush(self, recovered=False):
        try:
            yield self.flush_once()
        except Exception as e:
            self._connection.connect()
            raise thrift.transport.TTransport.TTransportException(
                message=str(e))

    @gen.coroutine
    def flush_once(self):
        if self._properties is not None:
            props = pika.BasicProperties(
                correlation_id=self._properties.correlation_id)
            self._channel.basic_publish(exchange='',
                                        routing_key=self._properties.reply_to,
                                        properties=props,
                                        body=self._wbuf.getvalue())
            if self._method is not None:
                self._channel.basic_ack(
                    delivery_tag=self._method.delivery_tag)
        else:
            with (yield self._lock.acquire()):
                props = pika.BasicProperties(correlation_id=str(uuid.uuid4()),
                                             reply_to=self._reply_to)

                if self._message_expiration:
                    props.expiration = str(self._message_expiration * 1000)

                self._channel.basic_publish(exchange=self._exchange_name,
                                            routing_key=self._routing_key,
                                            properties=props,
                                            body=str(self._wbuf.getvalue()))
        self._wbuf = StringIO()
