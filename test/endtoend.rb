require 'blizzard/data_node'
require 'blizzard/master'
require 'blizzard/client'
require 'rpc'
require 'test/unit'

include Blizzard
include RPC
include Transport

class EndToEndTest < Test::Unit::TestCase

  @@setup = false
  
  attr_reader :master, :client, :data_node

  def setup
    return if @@setup
    @@setup = true
    @@master = Master::Master.new(2)
    @@master_server = Master::MasterServer.new(@@master, "localhost", 1211)
    @@data_node1 = DataNode::DataNode.new(DataNode::MasterDummy.new(
        UDPTransport.new, "localhost", 1211, "localhost", 1212))
    @@data_node_server1 = DataNode::DataNodeServer.new(@@data_node1, "localhost", 1212)
    @@data_node2 = DataNode::DataNode.new(DataNode::MasterDummy.new(
        UDPTransport.new, "localhost", 1211, "localhost", 1213))
    @@data_node_server2 = DataNode::DataNodeServer.new(@@data_node1, "localhost", 1213)
    @@client = Client::Client.new(Client::MasterDummy.new(UDPTransport.new, "localhost", 1211))
    
    tm = Thread.new { @@master_server.start }
    td1 = Thread.new { @@data_node_server1.start }
    td2 = Thread.new { @@data_node_server2.start }
    @@data_node1.master.add_node(1, @data_node1)
    @@data_node2.master.add_node(2, @data_node2)
  end

  def test_add_node
    assert_equal(2, @@master.data_nodes.size)
  end

  def test_enqueue_dequeue
    @@client.dist_enqueue("hello world")
    assert_equal(1, @@data_node1.data.size)
    assert_equal("hello world", @@data_node1.data.values[0])
    
    assert_equal("hello world", @@client.dist_dequeue)
    assert_equal(0, @@data_node1.data.size)
  end

end

if __FILE__ == $0
  require 'test/unit/testsuite'
  require 'test/unit/ui/console/testrunner.rb'
  Test::Unit::UI::Console::TestRunner.run(EndToEndTest)
end
