$LOAD_PATH.unshift 'gen-rb'

require 'scrapper'
require 'thrift'
require 'thrift/amqp/ruby'

transport = Thrift::AmqpRpcClientTransport.new(
  'thrift', exchange: 'rpc-server-exchange', host: 'localhost', port: '5672'
)
protocol = Thrift::JsonProtocol.new(transport)
client = Scrapper::Client.new(protocol)

transport.open

puts client.scrape('https://google.com').content
