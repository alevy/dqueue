require 'data_node'
require 'master'
require 'client'
require 'rpc'
require 'test/unit'

include DQueue
include RPC
include Transport

class EndToEndTest < Test::Unit::TestCase

  attr_reader :master, :client, :data_node

  def setup
    @master = Master::Master.new
    @master_server = Master::MasterServer.new(@master, "localhost", 1211)
    @data_node = DataNode::DataNode.new(DataNode::MasterDummy.new(
        UDPTransport.new, "localhost", 1231, "localhost", 1212))
    @data_node_server = DataNode::DataNodeServer.new(@data_node, "localhost", 1212)
    @client = Client::Client.new(Client::MasterDummy.new(UDPTransport.new, "localhost", 1211))
    
    tm = Thread.new { @master_server.start }
    td = Thread.new { @data_node_server.start }
    puts tm.inspect
    puts td.inspect
    @data_node.master.add_node(1, nil)
  end

#  def teardown
#    @master_server.stop
#    @data_node_server.stop
#  end

#  def test_add_node
#    assert_equal(1, master.data_nodes.size)
#  end

  def test_enqueue
    @client.dist_enqueue("hello world")
    assert_equals(1, @data_node.data.size)
    assert_equals("hello world", @data_node.data.values[0])
  end

end

if __FILE__ == $0
  require 'test/unit/testsuite'
  require 'test/unit/ui/console/testrunner.rb'
  Test::Unit::UI::Console::TestRunner.run(EndToEndTest)
end
