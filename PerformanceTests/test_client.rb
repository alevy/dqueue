require 'PerformanceTests\single_node_queue'

class TestClient
  
  def initialize
    @server = SingleNodeQueue.new
  end
  
  
  def start(command)
    startTime = Time.now
  
    if command.eql?("small")   
      #small test: enqueue a whole bunch of integers
      500000.times do
        @server.enqueue(5)
      end
      
      500000.times do
        @server.dequeue
      end
    
    
    elsif command.eql?("medium") 
      #medium test: enqueue a whole bunch of small text files (4Kb)
      10000.times do
        @server.enqueue(file = File.new("PerformanceTests\\small_file.txt"))
        file.close
      end
      
      10000.times do
        @server.dequeue
      end
    
    elsif command.eql?("large") 
      #large test: enqueue a bunch of large files (20Mb)
      30.times do
        @server.enqueue(file = File.new("PerformanceTests\\large_file"))
        file.close
      end
      
      30.times do
        @server.dequeue
      end
    
    end
    
    
    endTime = Time.now
    
    return endTime - startTime
  end
  
end
