require 'data_node'
require 'deque'

class Master
  
  def initialize
    @data_nodes = Array.new
    @unique_id = 0
    @logical_queue = Deque.new 
    @pending_enqueues = Hash.new
    @pending_dequeues = Hash.new
  end
  
  #add a data node that the master can use
  def add_node(node)
    @data_nodes << node
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
        
        return [hashed_value, nodes_to_contact]
    end

    #start dequeue.  Grab an id from the queue, and
    #return that id along with the node to pull it from
    def start_dequeue()
        # generate unique client key and send it to nodes,
        # or an approach that results in something similar
        hashed_value = @logical_queue.dequeue
        @pending_dequeues[ hashed_value ] = true
    
        nodes_to_contact = get_nodes_to_store hashed_value
        
        return [hashed_value, nodes_to_contact]
    end
    
    def abort_dequeue(hashed_value)
        if @pending_dequeues.has_key? hashed_value
            @logical_queue.enqueue_as_head hashed_value
            @pending_dequeues.delete hashed_value
        end
    end
    
    def abort_enqueue(hashed_value)
        if @pending_enqueues.has_key? hashed_value
            @pending_enqueues.delete hashed_value
            # notify any nodes they can delete a waiting value
        end
    end
    
    #finalize enqueue.  Put this id on the actual queue.
    def finalize_enqueue(hashed_value)
        if @pending_enqueues.has_key? hashed_value
            @logical_queue.enqueue hashed_value
        end
    end
  
    #finalize dequeue.  Remove the given item id from
    #the pending dequeue list, and delete that data from
    #nodes on which it is stored.
    def finalize_dequeue(hashed_value)
        if @pending_dequeues.has_key? hashed_value
            @pending_dequeues.delete hashed_value
            # notify nodes about dequeue so they can remove the data at some point
            nodes_to_notify = get_nodes_to_store hashed_value
            nodes_to_notify.each do |current_node|
            	current_node.delete_data hashed_value
            end
        end
    end
    
    def get_nodes_to_store(data_key)
        # look up nodes which store data associated with the given hash
        return [@data_nodes[0]]
    end
end