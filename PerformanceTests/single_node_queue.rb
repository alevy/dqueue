require 'rpc'

class SingleNodeQueue
  
  def initialize
    @queue = Array.new
    @file = File.new("single_node_log.log", "a+")
  end
  
  def enqueue(item)
    @file.puts("12345678901234567890")
    @queue << item
    item
  end
  
  def dequeue
    @queue.shift
    @file.puts("12345678901234567890")
  end
  
  
end

class SingleNodeQueueServer < RPC::Server
  def initialize(q, host, port, transport = RPC::Transport::TCPTransport.new)
    wrapper = RPC::Wrapper.new(q, :enqueue, :dequeue)
    super(transport, wrapper, host, port)
  end
end

class SingleNodeQueueDummy < RPC::Dummy
end

if __FILE__ == $0
  q = SingleNodeQueue.new
  server = SingleNodeQueueServer.new(q, ENV["HOSTNAME"], 3456)
  server.start
end