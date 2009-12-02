require 'master'
require 'client'
require 'data_node'

include DQueue::Master
include DQueue::Client
include DQueue::DataNode

class Simulator

  @master = Master.new
  @data_node = DataNode.new(@master)
  @data_node2 = DataNode.new(@master)
  @data_node3 = DataNode.new(@master)
  @client = Client.new(@master)
  
  @master.add_node(1, @data_node)
  @master.add_node(2, @data_node2)
  @master.add_node(3, @data_node3)

  
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
