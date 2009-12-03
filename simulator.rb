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
  @data_node4 = DataNode.new(@master)
  @client = Client.new(@master)
  
  @master.add_node(1, @data_node)
  @master.add_node(2, @data_node2)
  @master.add_node(3, @data_node3)
  @master.add_node(4, @data_node4)

  
  @client.dist_enqueue("Item one!")
  puts "Item 1 enqueued..."
  puts "killing a node!"
  @data_node.kill
  sleep 10
  @client.dist_enqueue("Item two!")
  puts "Item 2 enqueued..."
  sleep 10
  @client.dist_enqueue("Item three!")
  puts "Item 3 enqueued..."
  sleep 10
  @client.dist_enqueue(4)
  puts "Item 4 enqueued..."
  
  puts @client.dist_dequeue
  puts @client.dist_dequeue
  puts @client.dist_dequeue
  puts @client.dist_dequeue
  puts @client.dist_dequeue

end
