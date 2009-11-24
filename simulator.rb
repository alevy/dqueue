require 'master'
require 'client'
require 'data_node'

class Simulator

  @master = Master.new
  @data_node = DataNode.new(@master)
  @client = Client.new(@master)
  
  @master.add_node(1, @data_node)
  
  @client.dist_enqueue("Item one!")
  @client.dist_enqueue("Item two!")
  @client.dist_enqueue("Item three!")
  @client.dist_enqueue(4)
  
  puts @client.dist_dequeue
  puts @client.dist_dequeue
  puts @client.dist_dequeue
  puts @client.dist_dequeue
  puts @client.dist_dequeue

end
