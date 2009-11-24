require 'socket'
 
module RPC
  module Transport
    class UDPTransport
    
      UDP_RECV_TIMEOUT = 3
      
      def initialize(hash = {})
        @timeout = hash[:timeout] || UDP_RECV_TIMEOUT
        @serializer = hash[:serializer] || Marshal
      end
      
      def bind(address, port)
        @socket = UDPSocket.new
        @socket.bind(address, port)
      end
      
      def listen
        text, sender = @socket.recvfrom(1024)
        begin
          result = yield(*@serializer.load(text))
          @socket.send(@serializer.dump(result), 0, *sender.values_at(3,1))
        rescue
          @socket.send(@serializer.dump($!), 0, *sender.values_at(3,1))
        end
      end
      
      def send_msg(address, port, msg, *args)
        socket = @socket || UDPSocket.new
        socket.send(@serializer.dump([msg,args]), 0, address, port)
        resp = socket.recvfrom(1024) if select([socket], nil, nil, @timeout)
        raise "Connection Timed Out" unless resp
        result = @serializer.load(resp[0])
        raise result if result.is_a?(Exception)
        return result
      end
    end
  end
  
  class Wrapper
    
    attr_reader :obj
    
    def initialize(obj, *allowed_calls)
      @obj = obj
      @allowed_calls = allowed_calls.map {|c| c.to_sym}
    end
    
    def method_missing(method, *args)
      method = method.to_sym
      if @allowed_calls.include?(method)
        obj.send(method, *args)
      else
        return Exception.new("Method not allowed")
      end
    end
    
  end
  
  class Dummy
    
    attr_reader :transport, :host, :port
    
    def initialize(transport, host, port)
      @transport = transport
      @host = host
      @port = port
    end
    
    def method_missing(method, *args)
      send_msg(method, *args)
    end
    
    def send_msg(method, *args)
      transport.send_msg(host, port, method, *args)
    end
  end
  
  class Server
    
    def initialize(transport, wrapper, address, port)
      @wrapper = wrapper
      @transport = transport
      @transport.bind(address, port)
    end
    
    def start
      loop do
        @transport.listen {|method, args| @wrapper.send(method, *args)}
      end
    end
  end
  
end
