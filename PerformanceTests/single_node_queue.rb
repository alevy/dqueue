class SingleNodeQueue
  
  
  def initialize
    @queue = Array.new
  end
  
  def enqueue(item)
    @queue << item
  end
  
  def dequeue
    @queue.shift
  end
  
  
end

