require 'socket'

module RPC

  class RPCDummy

    def initialize(address, port)
      @address = address
      @port = port
    end

    def method_missing(msg, *args)
      socket = UDPSocket.new
      socket.connect(@address, @port)
      socket.send(Marshal.dump([msg,args]), 0)
      resp = socket.recvfrom(1024) if select([socket], nil, nil, @timeout)
      raise "Connection Timed Out" unless resp
      result = Marshal.load(resp[0])
      socket.close
      raise result if result.is_a?(Exception)
      return result
    end

  end

  class RPCServer
    
    def initialize(obj, *allowed_calls)
      @obj = obj
      @allowed_calls = allowed_calls
    end

    def init(address, port)
      @socket = UDPSocket.new
      @socket.bind(address, port)
    end

    def listen
      text, sender = @socket.recvfrom(1024)
      method, args = Marshal.load(text)
      begin
        if @allowed_calls.include?(method.to_sym)
          result = @obj.send(method, *args)
          @socket.send(Marshal.dump(result), 0, *sender.values_at(3,1))
        else
          @socket.send(Marshal.dump(Exception.new("Method not accessible")), 0, *sender.values_at(3,1))
        end
      rescue
        @socket.send(Marshal.dump($!), 0, *sender.values_at(3,1))
      end
    end

  end

end
