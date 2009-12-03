require 'socket'
require 'timeout'
 
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

      def close
        @socket.close
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
        resp = socket.recvfrom(1024)
        result = @serializer.load(resp[0])
        raise result if result.is_a?(Exception)
        return result
      end
    end
    
    class TCPTransport
    
      TCP_RECV_TIMEOUT = 3
      
      def initialize(hash = {})
        @timeout = hash[:timeout] || TCP_RECV_TIMEOUT
        @serializer = hash[:serializer] || Marshal
      end
      
      def bind(address, port)
        @socket = TCPServer.new(address, port)
      end

      def close
        @socket.close
      end
      
      def listen
        begin
          session = @socket.accept_nonblock
        rescue
          retry
        end
        text = session.read
        begin
          result = yield(*@serializer.load(text))
          session.write(@serializer.dump(result))
        rescue
          session.write(@serializer.dump($!))
        end
        session.close
      end
      
      def send_msg(address, port, msg, *args)
        socket = TCPSocket.new(address, port)
        socket.write(@serializer.dump([msg,args]))
        socket.close_write
        resp = nil
        begin  
          timeout(@timeout) do
            resp = socket.read
          end
        rescue
          raise "Connection Timed Out"
        end
        socket.close
        result = @serializer.load(resp)
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
    
    attr_reader :transport, :host, :port, :localhost, :localport
    
    def initialize(transport, host, port, localhost = nil, localport = nil)
      @transport = transport
      @host = host
      @port = port
      @localhost = localhost
      @localport = localport
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
      @address = address
      @port = port
    end
    
    def start
      @transport.bind(@address, @port)
      @continue = true
      while @continue do
        @transport.listen {|method, args| @wrapper.send(method, *args)}
      end
    end

    def stop
      @continue = false
      @transport.close
    end
  end
  
end
