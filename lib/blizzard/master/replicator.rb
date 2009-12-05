module Blizzard
  module Master
     class Replicator
    
      attr_reader :rep_thresh, :heartbeats
      
      def initialize(master, logger)
          @data_to_nodes = Hash.new
          @nodes_to_data = Hash.new
          @master = master
          @heartbeats = Hash.new
          @logger = logger
      end
      
      def start
        Thread.abort_on_exception = true
        Thread.new do
          while true do
            sleep 10
            check_heartbeats
          end
        end
      end
      
      def recover_from_log(log_file)
        log_file.each do |log_line|
          log_line_words = log_line.split(Logger::DELIMITER)
          
          operation_type = log_line_words[0]
          
          if operation_type == BlizzardLogger::REMOVE_NODE_TO_DATA
            @nodes_to_data.delete(Marshal.load(log_line_words[1]))
          elsif operation_type == BlizzardLogger::REMOVE_DATA_TO_NODE
            @data_to_nodes[log_line_words[1]].delete(Marshal.load(log_line_words[2]))
          elsif operation_type == BlizzardLogger::ADD_REPLICA
            add_replica(log_line_words[1], Marshal.load(log_line_words[2]), true)
          elsif operation_type == BlizzardLogger::CLEAR_REPLICAS
            clear_replicas(log_line_words[1], true)
          end
        end
      end
      
      def get_heartbeat(node)
        @heartbeats[node] = Time.now
      end
        
      def check_heartbeats
        $stderr.puts "checking heartbeats..."
        @heartbeats.each do |node, value|
          if Time.now - value > 10
            $stderr.puts "replicating node!"
            @master.remove_node(node)
            
            data = get_data_list(node)
            data.each do |element|
              @logger.log_remove_data_to_node(element, Marshal.dump(node))
              @data_to_nodes[element].delete(node)
            end
            @logger.log_remove_node_to_data(Marshal.dump(node))
            @nodes_to_data.delete(node)
             
            replicate_node(node)
            @heartbeats.delete(node)
            $stderr.puts "successfully replicated node"
          end
        end
      end
    
    def replicate(item_id, num)
      num.times do
        replicate_once(item_id)      end
    end
     
    #replicate the given item ID on an additional node
    def replicate_once(item_id)
      current_nodes = find_nodes(item_id)
      possible_nodes = @master.data_nodes.values.select {|n| not current_nodes.include?(n)}
      target_node = possible_nodes[(rand * possible_nodes.size).floor]
      
      current_nodes[0].replicate_data(item_id, target_node)
      
      #TODO if the above line failed, re-start this method.
      add_replica(item_id, target_node)
    end
      
     #find all nodes this item ID is currently stored on
    def find_nodes(item_id)
      return @data_to_nodes[item_id]
    end
    
    #choose the next node to replicate on
    def next_replica
      return @master.data_nodes[@master.data_nodes.keys[(rand * @master.data_nodes.size).floor]]
    end
    
    #replicate all data on a node
    def replicate_node(node_id)
      data_to_replicate = get_data_list(node_id)
      data_to_replicate.each do |data_id|
        replicate(data_id)
      end
    end
    
    #get the list of data elements stored on this node
    def get_data_list(node_id)
      if @nodes_to_data[node_id].nil?
        return []
      else
        return @nodes_to_data[node_id]
      end
    end
    
    #mark this item as stored at this node
    def add_replica(item_id, target_node, recovery_mode = false)
      @logger.log_add_replica(item_id, Marshal.dump(target_node)) unless recovery_mode
      if @nodes_to_data[target_node].nil?
        @nodes_to_data[target_node] = [item_id]
      else
        @nodes_to_data[target_node] = @nodes_to_data[target_node] << item_id
      end
      
      if @data_to_nodes[item_id].nil?
        @data_to_nodes[item_id] = [target_node]
      else
        @data_to_nodes[item_id] = @data_to_nodes[item_id] << target_node
      end
    end
    
    #remove the metadata info about this item, it's no longer
    #needed.
      def clear_replicas(item_id, recovery_mode = false)
        @logger.log_clear_replicas(item_id) unless recovery_mode
        nodes = find_nodes(item_id)
        nodes.each do |node|
          @nodes_to_data[node].delete(item_id)
        end
          @data_to_nodes.delete(item_id)
      end
    end
  end
end
