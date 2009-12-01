require 'rpc'
require 'data_node'
class Master
  
  def initialize
    @data_nodes = Hash.new
    @unique_id = 0
    @logical_queue = Array.new 
    @pending_enqueues = Hash.new
    @pending_dequeues = Hash.new
    
    #For use in replication handling
    @data_to_nodes = Hash.new
    @nodes_to_data = Hash.new
    @rep_thresh = 3
  end
  
  #add a data node that the master can use
  def add_node(node_id, node)
    @data_nodes[node_id] = node
  end
  
  #start enqueue.  Decide which data nodes to store on,
  #and return the unique ID for this data item.
  def start_enqueue()
    # generate unique client key and send it to nodes,
    # or an approach that results in something similar
    @unique_id = @unique_id + 1
    hashed_value = @unique_id
    @pending_enqueues[ hashed_value ] = true
    
    nodes_to_contact = get_nodes_to_store hashed_value
    add_replica(hashed_value, nodes_to_contact[0])
    
    return [hashed_value, nodes_to_contact]
  end

  #start dequeue.  Grab an id from the queue, and
  #return that id along with the node to pull it from
  def start_dequeue
    # generate unique client key and send it to nodes,
    # or an approach that results in something similar
    hashed_value = @logical_queue.shift
    @pending_dequeues[ hashed_value ] = true

    nodes_to_contact = find_nodes(hashed_value)
    
    return [hashed_value, nodes_to_contact]
  end
  
  def abort_dequeue(hashed_value)
    if @pending_dequeues.has_key? hashed_value
      @logical_queue.unshift(hashed_value)
      @pending_dequeues.delete hashed_value
      return true
    else
      return false
    end
  end
  
  def abort_enqueue(hashed_value)
    if @pending_enqueues.has_key?(hashed_value)
      @pending_enqueues.delete(hashed_value)
      clear_replicas(hashed_value)
      # notify any nodes they can delete a waiting value
      
      return true
    else
      return false
    end
  end
  
  #finalize enqueue.  Put this id on the actual queue, and
  #initialize necessary replicas.
  def finalize_enqueue(hashed_value)
      if @pending_enqueues.has_key?(hashed_value)
        (@rep_thresh - 1).times do
          replicate(hashed_value)
        end
        
        @logical_queue << hashed_value
      
        
        return true
      else
        return false
      end
  end

  #finalize dequeue.  Remove the given item id from
  #the pending dequeue list, and delete that data from
  #nodes on which it is stored.
  def finalize_dequeue(hashed_value)
    if @pending_dequeues.has_key?(hashed_value)
      @pending_dequeues.delete(hashed_value)
      # notify nodes about dequeue so they can remove the data at some point
      nodes_to_notify = find_nodes(hashed_value)
      nodes_to_notify.each do |current_node|
        current_node.delete_data(hashed_value)
     end
     
     clear_replicas(hashed_value)
     
      return true
    else
      return false
    end
  end
  
  def get_nodes_to_store(data_key)
      # look up nodes which store data associated with the given hash
      #currently returns a single random node
      return [@data_nodes[@data_nodes.keys[(rand * @data_nodes.size).floor]]]
  end
  
  #replicate the given item ID on an additional node
  def replicate(item_id)
    current_nodes = find_nodes(item_id)
    target_node = next_replica
    while(current_nodes.include?(target_node)) do
      target_node = next_replica
    end
    
    target_node.add_data(item_id, current_nodes[0].get_data(item_id))
    add_replica(item_id, target_node)
  end
  
 #find all nodes this item ID is currently stored on
  def find_nodes(item_id)
    return @data_to_nodes[item_id]
    
  end
  
  #choose the next node to replicate on
  def next_replica
    return @data_nodes[@data_nodes.keys[(rand * @data_nodes.size).floor]]
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
    nodes = find_nodes(item_id)
    nodes.each do |node|
      @nodes_to_data[node].delete(item_id)
    end
    @data_to_nodes.delete(item_id)
  end
end

class MasterServer < RPC::Server

  def initialize(master, host, port, transport = RPC::Transport::UDPTransport.new)
    wrapper = RPC::Wrapper.new(master, :add_node, :start_enqueue, :start_dequeue,
              :finalize_enqueue, :finalize_dequeue, :abort_dequeue, :abort_enqueue)
    super(transport, wrapper, host, port)
  end
  
end