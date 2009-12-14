require 'blizzard/client'

include Blizzard
include RPC
include Transport

  
def trial(num, threads)
  client = Client::Client.new(Client::MasterDummy.new(UDPTransport.new, "smurf.cs.washington.edu", 9876))
  
  startenq = Time.now
  num.times do |i|
    client.dist_enqueue(i.to_s)
  end
  endenq = Time.now
  
  startdeq = Time.now
  num.times do
    client.dist_dequeue
  end
  enddeq = Time.now
  
  [(endenq - startenq) / threads, (enddeq - startdeq) / threads]  
end

def expt(num, threads)
  results = []
  ts = []
  threads.times do
    ts << Thread.new { results << trial(num, threads) }
  end
  ts.each {|t| t.join }
  return results
end

results = []
((ARGV[0].to_i)..(ARGV[1].to_i)).each do |i|
  $stderr.puts(i)
  r = [2**i, expt(100,2**i)]
  results << r
end

results.each do |i,r|
  puts "#{i},#{r.inject([0,0]) {|s,v| [s[0] + v[0],s[1] + v[1]] }.map {|v| v / r.size}.join(",")}"
end
