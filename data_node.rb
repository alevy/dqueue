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