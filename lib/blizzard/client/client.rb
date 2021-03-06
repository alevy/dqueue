require 'rpc'

module Blizzard
  module Client
    class Client
      def initialize(master)
        @master_node = master
      end
      
      #add a data item to the distributed queue.
      def dist_enqueue(data)
        info = @master_node.start_enqueue
        data_id = info[0]
        nodes = info[1]
        nodes[0].add_data(data_id, data)
        @master_node.finalize_enqueue(data_id)
      end
      
      #remove an item from the distributed queue.
      def dist_dequeue
        info = @master_node.start_dequeue
        return nil if info.nil?
        data_id = info[0]
        nodes = info[1]
        result = nodes[0].get_data(data_id)
        @master_node.finalize_dequeue(data_id)
        
        return result
      end
      
    end

    class MasterDummy < RPC::Dummy
      def start_enqueue
        data_id, nodes = send_msg(:start_enqueue)
        nodes = nodes.map do |node|
          DataNodeDummy.new(RPC::Transport::TCPTransport.new, node[:host], node[:port])
        end
        return [data_id, nodes]
      end

      def start_dequeue
        data_id, nodes = send_msg(:start_dequeue)
        return nil if nodes.nil?
        nodes = nodes.map do |node|
          DataNodeDummy.new(RPC::Transport::TCPTransport.new, node[:host], node[:port])
        end
        return [data_id, nodes]
      end
    end

    class DataNodeDummy < RPC::Dummy
    end
  end
end
