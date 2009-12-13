require 'blizzard/data_node'
require 'blizzard/master'
require 'blizzard/client'
require 'rpc'
require 'test/unit'

include Blizzard
include RPC
include Transport

class RecoveryTest < Test::Unit::TestCase
  @@setup = false
  
  attr_reader :master, :client, :data_node

  def setup
    return if @@setup
    @@setup = true
    set_up_everything(1211)
  end

  def test_enqueue_dequeue
    @@client.dist_enqueue("hello world")
#    assert_equal("hello world", @@client.dist_dequeue)
    
#    puts(@@master.inspect + "\n\n\n")
    # kill master
    #@@master_server.stop
    Thread.kill(@@master_thread)
    
    # restart master
    new_master_port = 1215
    data_node_port1 = 1212
    data_node_port2 = 1213
    data_node_port3 = 1214
    create_new_master(1215)
    @@data_node1.set_master(new_master_dummy(new_master_port, data_node_port1))
    @@data_node2.set_master(new_master_dummy(new_master_port, data_node_port2))
    @@data_node2.set_master(new_master_dummy(new_master_port, data_node_port3))
    @@client = Client::Client.new(Client::MasterDummy.new(UDPTransport.new, "localhost", new_master_port))
    
#    puts(@@master.inspect)
    
    assert_equal("hello world", @@client.dist_dequeue)
    
    @@master.clear_log
  end
  
  
  
  private
  def enqueue_map
  end

  def set_up_everything(port = 1211)
    create_new_master(port)
    @@data_node1 = DataNode::DataNode.new(new_master_dummy(port, port + 1))
    @@data_node_server1 = DataNode::DataNodeServer.new(@@data_node1, "localhost", port + 1)
    @@data_node2 = DataNode::DataNode.new(new_master_dummy(port, port + 2))
    @@data_node_server2 = DataNode::DataNodeServer.new(@@data_node2, "localhost", port + 2)
    @@data_node3 = DataNode::DataNode.new(new_master_dummy(port, port + 3))
    @@data_node_server3 = DataNode::DataNodeServer.new(@@data_node3, "localhost", port + 3)
    @@client = Client::Client.new(Client::MasterDummy.new(UDPTransport.new, "localhost", port))
    
    td1 = Thread.new { @@data_node_server1.start }
    td2 = Thread.new { @@data_node_server2.start }
    td3 = Thread.new { @@data_node_server3.start }
    @@data_node1.master.add_node(1, @@data_node1)
    @@data_node2.master.add_node(2, @@data_node2)
    @@data_node3.master.add_node(3, @@data_node3)
  end
  
  def new_master_dummy(master_port, data_node_port)
    return DataNode::MasterDummy.new(
      UDPTransport.new, "localhost", master_port, "localhost", data_node_port)
  end
  
  def create_new_master(port = 1211)
    @@master = Master::Master.new
    @@master_server = Master::MasterServer.new(@@master, "localhost", port)
    @@master_thread = Thread.new { @@master_server.start }
    
#    @@data_node1.set_master(@@master_server)
#    @@data_node2.set_master(@@master_server)
#    @@data_node3.set_master(@@master_server)
  end
end