require 'rpc'

module DQueue
  module DataNode
    class DataNode
      def initialize(master)
        @data = Hash.new
        @master = master
      end
      
      # store a data item on this node.
      def add_data(key, value)
        @data[key] = value
      end
      
      #get the given data item
      def get_data(key)
        return @data[key]
      end
      
      #delete the given data item
      def delete_data(key)
        @data.delete(key)
      end
      
    end

    class MasterDummy < RPC::Dummy
      def add_node(id, node)
        send_msg(:add_node, id, {:host => @host, :port => @port})
      end
    end

    class DataNodeServer < RPC::Server
      
      def initialize(master, host, port, transport = RPC::Transport::TCPTransport.new)
        wrapper = RPC::Wrapper.new(master, :add_data, :get_data, :delete_data)
        super(transport, wrapper, host, port)
      end
      
    end
  end
end
