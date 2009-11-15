class Master
  require 'data_node'
  
  def initialize
    @data_nodes = Array.new
    @unique_id = 0;
    @the_queue = Queue.new
    @pending_enq = Hash.new
    @pending_deq = Hash.new
  end
  
  #add a data node that the master can use
  def add_node(node)
    @data_nodes << node
  end
  
  #pending enqueue.  Decide which data nodes to store on,
  #and return the unique ID for this data item.
  def pending_enqueue
    used_id = @unique_id
    target_node = @data_nodes[0]
    @unique_id += 1
    @pending_enq[used_id] = target_node
    
    return [used_id, target_node]
  end
  
  #finalize enqueue.  Put this id on the actual queue.
  def finalize_enqueue(id)
    @pending_enq.delete(id)
    @the_queue.enq(id)
  end
  
  #pending dequeue.  Grab an id from the queue, and
  #return that id along with the node to pull it from
  def pending_dequeue
    if @the_queue.empty?
      return nil
    else
      deq_id = @the_queue.deq
      deq_node = @data_nodes[0]
      @pending_deq[deq_id] = deq_node 
      return [deq_id, deq_node]
    end
  end
  
  #finalize dequeue.  Remove the given item id from
  #the pending dequeue list, and delete that data from
  #nodes on which it is stored.
  def finalize_dequeue(id)
    data_nodes = @pending_deq[id]
    @pending_deq.delete(id)
    data_nodes.delete_data(id)
  end
  
  
  
end








