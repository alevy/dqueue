module DQueue
  class Replicator
  
    def initialize(master)
        @data_to_nodes = Hash.new
        @nodes_to_data = Hash.new
        @master = master
        @heartbeats = Hash.new
        Thread.abort_on_exception = true
        Thread.new{while true do sleep 10; check_heartbeats; end}
    end
  
    def get_heartbeat(node)
      @heartbeats[node] = Time.now
    end
    
    def check_heartbeats
      puts "checking heartbeats..."
      @heartbeats.each do |key, value|
        if Time.now - value > 10
          puts "replicating node!"
          replicate_node(key)
          @heartbeats.delete(key)
          puts "successfully replicated node"
        end
      end
    end
     
    #replicate the given item ID on num additional nodes
    def replicate(item_id, num)
      num.times do
        replicate_once(item_id)
      end
    end
    
    #replicate the given item ID on num additional nodes
    def replicate_once(item_id)
      current_nodes = @data_to_nodes[item_id]
      target_node = next_replica
      while(current_nodes.include?(target_node)) do
        target_node = next_replica
      end
      
      target_node.add_data(item_id, current_nodes[0].get_data(item_id))
      add_replica(item_id, target_node)
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
      return @nodes_to_data[node_id]
      
    end

    #mark this item as stored at this node
    def add_replica(item_id, target_node)
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
    def clear_replicas(item_id)
      nodes = @data_to_nodes[item_id]
      nodes.each do |node|
        @nodes_to_data[node].delete(item_id)
      end
      @data_to_nodes.delete(item_id)
    end
  end
end