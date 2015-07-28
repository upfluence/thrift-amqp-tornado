import logging
import copy

import pika
from pika.adapters import TornadoConnection
from tornado import gen
from thrift.transport.TTransport import TMemoryBuffer
from thrift.Thrift import TMessageType
from transport import TAMQPTornadoTransport
import constant

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TAMQPTornadoServer(object):
    def __init__(self, processor, iprot_factory, oprot_factory=None, *args,
                 **kwargs):
        self._processor = processor
        self._iprot_factory = iprot_factory
        self._oprot_factory = oprot_factory if oprot_factory else iprot_factory
        self._connection = None
        self._channel = None
        self._url = kwargs.get('url', constant.DEFAULT_URL)
        self._exchange_name = kwargs.get('exchange_name',
                                         constant.EXCHANGE_NAME)
        self._routing_key = kwargs.get('routing_key', constant.ROUTING_KEY)
        self._queue_name = kwargs.get('queue_name', constant.QUEUE_NAME)
        self._prefetch = kwargs.get('prefetch', 0)

    def start(self):
        logger.info("Starting the connection")
        self._connection = TornadoConnection(pika.URLParameters(self._url),
                                             self.on_connection_open,
                                             self.on_connection_error,
                                             self.on_connection_error)

    @gen.coroutine
    def on_connection_error(self, *args, **kwargs):
        logger.info("Connection failed")
        yield gen.sleep(constant.TIMEOUT_RECONNECT)
        self.start()

    def on_connection_open(self, _):
        logger.info("Connection started")
        logger.info("Starting the channel")
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        logger.info("Channel started")
        logger.info(
            "Declaring exchange : {} of type {}".format(
                self._exchange_name, constant.EXCHANGE_TYPE))

        self._channel = channel
        self._channel.basic_qos(prefetch_count=self._prefetch)
        try:
            self._channel.exchange_declare(self.on_exchange_declared,
                                           self._exchange_name,
                                           constant.EXCHANGE_TYPE, False)
        except:
            self._channel.exchange_declare(self.on_exchange_declared,
                                           self._exchange_name,
                                           constant.EXCHANGE_TYPE, True)

    def on_exchange_declared(self, _):
        logger.info(
            "Exchange declared : {} of type {}".format(
                self._exchange_name, constant.EXCHANGE_TYPE))
        logger.info("Declaring queue: {}".format(self._queue_name))

        self._channel.queue_declare(self.on_queue_declared, self._queue_name)

    def on_queue_declared(self, _queue):
        logger.info("Queue declared : {}".format(self._queue_name))
        self._channel.queue_bind(self.on_binded, self._queue_name,
                                 self._exchange_name, self._routing_key)

    def on_binded(self, _bind):
        logger.info(
            "Queue {} binded to the {} exchange with the {} routing key".format(
                self._queue_name, self._exchange_name, self._routing_key))
        self._channel.basic_consume(self.on_message, self._queue_name)

    @gen.coroutine
    def on_message(self, _channel, method, properties, body):
        iprot = self._iprot_factory.getProtocol(TMemoryBuffer(body))
        iprot_dup = self._iprot_factory.getProtocol(TMemoryBuffer(body))

        try:
            type = iprot_dup.readMessageBegin()[1]
        except:
            self._channel.basic_ack(delivery_tag=method.delivery_tag)
            raise gen.Return()

        if type == TMessageType.ONEWAY:
            try:
                yield self._processor.process(iprot, None)
            except Exception as e:
                logger.error(e, exc_info=True)

            self._channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            trans = TAMQPTornadoTransport(channel=self._channel,
                                          properties=properties,
                                          method=method)
            oprot = self._oprot_factory.getProtocol(trans)
            try:
                yield self._processor.process(iprot, oprot)
            except Exception as e:
                logger.error(e, exc_info=True)
                self._channel.basic_ack(delivery_tag=method.delivery_tag)
