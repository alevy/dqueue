#!/usr/bin/env ruby

$LOAD_PATH.unshift File.dirname(__FILE__) + '/lib'

puts $LOAD_PATH.inspect

require 'blizzard/master'

include Blizzard

localhost = ENV["HOSTNAME"]
localport = ARGV[0] || 9876

@master = Master::Master.new

@master_server = Master::MasterServer.new(@master, localhost, localport)
  
@servt = Thread.new { @master_server.start }
  
@master.replicator.start
