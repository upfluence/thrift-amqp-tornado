from StringIO import StringIO
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
        self._reply_queue_name = kwargs.get('reply_queue_name', '')
        self._error_logger = kwargs.get('error_logger')
        self._callback_queue = Queue()
        self.io_loop = io_loop or ioloop.IOLoop.instance()
        self._closing = False
        self._starting = False

    @gen.coroutine
    def assign_queue(self):
        logger.info("Openning callback queue")
        result = yield gen.Task(self._channel.queue_declare,
                                queue=self._reply_queue_name,
                                exclusive=True,
                                auto_delete=True)
        logger.info("Callback queue openned")

        if self._reply_queue_name == '':
            self._reply_to = result.method.queue
        else:
            self._reply_to = self._reply_queue_name

        if self._lock.acquired():
            self._lock.release()

        self._consumer_tag = self._channel.basic_consume(
            self.on_reply_message, self._reply_to,
            consumer_tag=self._consumer_name)

        self._starting = False
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
        if self._starting:
            if callback:
                callback()
            return

        self._starting = True

        logger.info("Openning AMQP transport")
        if self._channel is not None and self._channel.is_open:
            logger.info("Already set")
            if callback:
                callback()
        else:
            logger.info("Openning connection")
            self._callback = callback
            self._lock.acquire()
            self._connection = TornadoConnection(pika.URLParameters(self._url),
                                                 self.on_connection_open,
                                                 self.on_connection_error,
                                                 self.on_connection_error)

    @gen.coroutine
    def on_connection_error(self, *args, **kwargs):
        self._starting = False
        logger.info("Connection failed")
        yield gen.sleep(constant.TIMEOUT_RECONNECT)
        self.open(self._callback)

    @gen.coroutine
    def readFrame(self):
        result = yield self._callback_queue.get()

        if issubclass(result.__class__, Exception):
            raise result

        raise gen.Return(result)

    @gen.coroutine
    def on_channel_close(self, *args):
        logger.info("Channel closed")

        yield gen.sleep(constant.TIMEOUT_RECONNECT)

        self._callback_queue.put(
            thrift.transport.TTransport.TTransportException(
                message='channel closed'))

        if not self._closing and not self._starting:
            if self._connection and self._connection.is_open:
                self._lock.acquire()
                self._connection.channel(on_open_callback=self.on_channel_open)
            else:
                self.open()

    def on_channel_open(self, channel):
        logger.info("Channel openned")
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_close)
        self.assign_queue()

    def close(self):
        self._closing = True
        if self._channel:
            if self._consumer_tag:
                self._channel.basic_cancel(consumer_tag=self._consumer_tag,
                                           nowait=True)
            self._channel.close()

    def isOpen(self):
        return self._channel is not None

    def read(self, _):
        assert False, "wrong stuff"

    @gen.coroutine
    def write(self, buf):
        with (yield self._lock.acquire()):
            if type(buf) is unicode:
                self._wbuf.write(buf.encode("utf-8"))
            else:
                self._wbuf.write(buf)

    @gen.coroutine
    def flush(self, recovered=False):
        try:
            yield self.flush_once()
            self._wbuf = StringIO()
        except Exception as e:
            yield gen.sleep(constant.TIMEOUT_RECONNECT)

            if not recovered:
                yield self.flush(True)
            else:
                if self._error_logger:
                    self._error_logger.capture_exception()

                logger.info(e, exc_info=True)
                self._wbuf = StringIO()
                raise thrift.transport.TTransport.TTransportException(
                    message=str(e))

    @gen.coroutine
    def flush_once(self):
        if self._properties is not None:
            props = pika.BasicProperties(
                correlation_id=self._properties.correlation_id)

            result = self._wbuf.getvalue()

            if type(result) is unicode:
                result = result.encode("utf-8")

            self._channel.basic_publish(exchange='',
                                        routing_key=self._properties.reply_to,
                                        properties=props,
                                        body=result)
            if self._method is not None:
                self._channel.basic_ack(
                    delivery_tag=self._method.delivery_tag)
        else:
            with (yield self._lock.acquire()):
                props = pika.BasicProperties(correlation_id=str(uuid.uuid4()),
                                             reply_to=self._reply_to)

                if self._message_expiration:
                    props.expiration = str(self._message_expiration * 1000)

                result = self._wbuf.getvalue()

                if type(result) is unicode:
                    result = result.encode("utf-8")

                self._channel.basic_publish(exchange=self._exchange_name,
                                            routing_key=self._routing_key,
                                            properties=props,
                                            body=result)
        self._wbuf = StringIO()
