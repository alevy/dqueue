require 'rpc'
require 'replicator'
require 'blizzard_logger'

module DQueue
  module Master
    class Master
      attr_reader :data_nodes
      
      attr_reader :data_nodes

      def initialize(rep_thresh = 3)
        @data_nodes = Hash.new
        @unique_id = 0
        @logical_queue = Array.new 
        @pending_enqueues = Hash.new
        @pending_dequeues = Hash.new
        @replicator = Replicator.new(self)
        @rep_thresh = rep_thresh
        @logger = BlizzardLogger.new
        
      end
      
      def get_heartbeat(node)
        @replicator.get_heartbeat(node)
      end
    
      
      def recover_from_log
        log_file = @logger.get_log_file
        
        log_file.each do |log_line|
          log_line_words = log_line.split(Logger::DELIMITER)
          
          # TODO bad style for tightly integrating the 
          # logger and master... can change later
          operation_type = log_line_words[0]
          queue_value = log_line_words[2]
          
          if operation_type == BlizzardLogger::ADD_NODE
            # data nodes aren't actually serializable yet
            #add_node log_line_words[1], 
            #  {:host => log_line_words[2], :port => log_line_words[3]}
          elsif operation_type == BlizzardLogger::START_ENQUEUE
            start_enqueue queue_value, true
          elsif operation_type == BlizzardLogger::FINALIZE_ENQUEUE
            finalize_enqueue queue_value, true
          elsif operation_type == BlizzardLogger::ABORT_ENQUEUE
            abort_enqueue queue_value, true
          elsif operation_type == BlizzardLogger::START_DEQUEUE
            start_dequeue queue_value, true
          elsif operation_type == BlizzardLogger::FINALIZE_DEQUEUE
            finalize_dequeue queue_value, true
          elsif operation_type == BlizzardLogger::ABORT_DEQUEUE
            abort_dequeue queue_value, true
          end
        end
      end
      
      #add a data node that the master can use
      def add_node(node_id, node, recovery_mode = false)
        @logger.log_add_node(node_id, node) unless recovery_mode
        @data_nodes[node_id] = node
        @replicator.get_heartbeat(node)
      end
      
      #start enqueue.  Decide which data nodes to store on,
      #and return the unique ID for this data item.
      def start_enqueue(hashed_value = @unique_id, recovery_mode = false)
        # generate unique client key and send it to nodes,
        # or an approach that results in something similar
        @unique_id = [@unique_id + 1, hashed_value + 1].max

        @logger.log_enqueue_start("enq" + hashed_value.to_s, hashed_value) unless recovery_mode
        @pending_enqueues[hashed_value] = true
        nodes_to_contact = get_nodes_to_store hashed_value
        @replicator.add_replica(hashed_value, nodes_to_contact[0])
        return [hashed_value, nodes_to_contact]
      end
    
      #start dequeue.  Grab an id from the queue, and
      #return that id along with the node to pull it from
      def start_dequeue(hashed_value = @logical_queue.shift, recovery_mode = false)
        # generate unique client key and send it to nodes,
        # or an approach that results in something similar
        return nil if hash_value == nil

        @logger.log_dequeue_start "dq" + hashed_value.to_s, hashed_value unless recovery_mode
        @pending_dequeues[ hashed_value ] = true
        nodes_to_contact = @replicator.find_nodes(hashed_value)
        return [hashed_value, nodes_to_contact]
      end
      
      def abort_dequeue(hashed_value, recovery_mode = false)
        if @pending_dequeues.has_key? hashed_value
          @logger.log_dequeue_abort "dq" + hashed_value.to_s, hashed_value unless recovery_mode
          @logical_queue.unshift(hashed_value)
          @pending_dequeues.delete hashed_value
          return true
        else
          return false
        end
      end
      
      def abort_enqueue(hashed_value, recovery_mode = false)
        if @pending_enqueues.has_key?(hashed_value)
          if !recovery_mode
            @logger.log_enqueue_abort "enq" + hashed_value.to_s, hashed_value
          end
          @pending_enqueues.delete(hashed_value)
          @replicator.clear_replicas(hashed_value)
          # notify any nodes they can delete a waiting value
          
          return true
        else
          return false
        end
      end
      
      #finalize enqueue.  Put this id on the actual queue, and
      #initialize necessary replicas.
      def finalize_enqueue(hashed_value, recovery_mode = false)
        return false unless @pending_enqueues.has_key?(hashed_value)
        @logger.log_enqueue_finalize "enq" + hashed_value.to_s, hashed_value unless recovery_mode
        
        @replicator.replicate(hashed_value, @rep_thresh - 1)
          
        @logical_queue << hashed_value
        return true
      end
    
      #finalize dequeue.  Remove the given item id from
      #the pending dequeue list, and delete that data from
      #nodes on which it is stored.
      def finalize_dequeue(hashed_value, recovery_mode = false)
        return false unless @pending_dequeues.has_key?(hashed_value)
        
        @logger.log_dequeue_finalize "dq" + hashed_value.to_s, hashed_value unless recovery_mode
        @pending_dequeues.delete(hashed_value)
        # notify nodes about dequeue so they can remove the data at some point
        nodes_to_notify = @replicator.find_nodes(hashed_value)
        nodes_to_notify.each do |current_node|
          current_node.delete_data(hashed_value)
        end
        @replicator.clear_replicas(hashed_value)
        return true
      end  
      
      def get_nodes_to_store(data_key)
          # look up nodes which store data associated with the given hash
          #currently returns a single random node
          return [@data_nodes[@data_nodes.keys[(rand * @data_nodes.size).floor]]]
      end
      
    end

    class MasterServer < RPC::Server

      def initialize(master, host, port, transport = RPC::Transport::UDPTransport.new)
        wrapper = RPC::Wrapper.new(master, :add_node, :start_enqueue, :start_dequeue,
                  :finalize_enqueue, :finalize_dequeue, :abort_dequeue, :abort_enqueue)
        super(transport, wrapper, host, port)
      end
      
    end
  end
end
