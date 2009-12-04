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
    @@master = Master::Master.new(1)
    @@master_server = Master::MasterServer.new(@@master, "localhost", 1211)
    @@data_node = DataNode::DataNode.new(DataNode::MasterDummy.new(
        UDPTransport.new, "localhost", 1211, "localhost", 1212))
    @@data_node_server = DataNode::DataNodeServer.new(@@data_node, "localhost", 1212)
    @@client = Client::Client.new(Client::MasterDummy.new(UDPTransport.new, "localhost", 1211))
    
    tm = Thread.new { @@master_server.start }
    td = Thread.new { @@data_node_server.start }
    @@data_node.master.add_node(1, @data_node)
  end

  def test_add_node
    assert_equal(1, @@master.data_nodes.size)
  end

  def test_enqueue_dequeue
    @@client.dist_enqueue("hello world")
    assert_equal(1, @@data_node.data.size)
    assert_equal("hello world", @@data_node.data.values[0])
    
    assert_equal("hello world", @@client.dist_dequeue)
    assert_equal(0, @@data_node.data.size)
  end

end

if __FILE__ == $0
  require 'test/unit/testsuite'
  require 'test/unit/ui/console/testrunner.rb'
  Test::Unit::UI::Console::TestRunner.run(EndToEndTest)
end
