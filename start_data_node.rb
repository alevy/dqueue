#!/usr/bin/env ruby

$LOAD_PATH.unshift File.dirname(__FILE__) + '/lib'

puts $LOAD_PATH.inspect

require 'blizzard/data_node'
require 'sha1'

include Blizzard
include RPC
include Transport

localhost = ENV["HOSTNAME"]
localport = ARGV[0] || 9876

data_node = DataNode::DataNode.new(DataNode::MasterDummy.new(
  UDPTransport.new, "romieu.cs.washington.edu", 9876, localhost, localport))

data_node_server = DataNode::DataNodeServer.new(data_node, localhost, localport)
  
data_node.master.add_node(SHA1.new("#{localhost}:#{localport}").to_s, data_node)

data_node_server.start
