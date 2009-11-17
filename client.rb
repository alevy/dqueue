class Client
  
  def initialize(master)
    @master_node = master
  end
  
  #add a data item to the distributed queue.
  def dist_enqueue(data)
    info = @master_node.pending_enqueue
    data_id = info[0]
    node = info[1]
    node.add_data(data_id, data)
    @master_node.finalize_enqueue(data_id)
  end
  
  #remove an item from the distributed queue.
  def dist_dequeue
    info = @master_node.pending_dequeue
    if info.nil?
      return nil
    end
    data_id = info[0]
    node = info[1]
    result = node.get_data(data_id)
    @master_node.finalize_dequeue(data_id)
    
    return result
  end
  
end

