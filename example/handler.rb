$LOAD_PATH.unshift 'gen-rb'

require 'scrapper'
require 'thrift'
require 'thrift/amqp/ruby'

class ScrapHandler
  def scrape(url)
    Blog.new(url: url, content: "Hi")
  end
end

handler = ScrapHandler.new
processor = Scrapper::Processor.new(handler)
pfactory= Thrift::JsonProtocolFactory
server = Thrift::AmqpRpcServer.new(processor,
                                   host: 'localhost',
                                   port: '5672',
                                   exchange: 'rpc-server-exchange',
                                   queue_name: 'thrift',
                                   protocol_factory: pfactory)

server.serve
