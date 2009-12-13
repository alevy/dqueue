require 'rpc'

module Blizzard
  module DataNode
    class DataNode

      attr_reader :master, :data      
      
      def initialize(master)
        @data = Hash.new
        @master = master
        Thread.abort_on_exception = true
        @heartbeat = Thread.new{while true do sleep 3; send_heartbeat; end}
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

      def replicate_data(key, node)
        node.add_data(key, @data[key])
      end
      
      def set_master(master)
        @master = master
      end
      
      def send_heartbeat
        @master.get_heartbeat(self)
      end
      
      def kill
        @heartbeat.kill
        @data = Hash.new
      end
      
    end
    
    class DataNodeDummy < RPC::Dummy
    end

    class MasterDummy < RPC::Dummy
      def add_node(id, node)
        send_msg(:add_node, id, {:host => @localhost, :port => @localport})
      end
      
      def get_heartbeat(node)
        send_msg(:get_heartbeat, {:host => @localhost, :port => @localport})
      end
    end
    
    class DataNodeWrapper < RPC::Wrapper
      def replicate_data(key, node)
        obj.replicate_data(key, DataNodeDummy.new(RPC::Transport::TCPTransport.new, node[:host], node[:port]))
      end
    end
    
    class DataNodeServer < RPC::Server
      
      def initialize(data_node, host, port, transport = RPC::Transport::TCPTransport.new)
        wrapper = DataNodeWrapper.new(data_node, :add_data, :get_data, :delete_data, :replicate_data)
        super(transport, wrapper, host, port)
      end
      
    end
  end
end
