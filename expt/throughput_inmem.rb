require 'rpc'

def trial(num)
  client = RPC::Dummy.new(RPC::Transport::TCPTransport.new, "romieu.cs.washington.edu", 3456)
  
  
  startenq = Time.now
  num.times do |i|
    client.enqueue(i.to_s)
  end
  endenq = Time.now
  
  startdeq = Time.now
  num.times do
    client.dequeue
  end
  enddeq = Time.now
  
  [endenq - startenq, enddeq - startdeq]  
end

t1 = Thread.new { puts "#{100} - #{trial(100).inspect}" }
t2 = Thread.new { puts "#{100} - #{trial(100).inspect}" }
#t3 = Thread.new { puts "#{100} - #{trial(100).inspect}" }
  

t1.join
t2.join
#t3.join

t1 = Thread.new { puts "#{1000} - #{trial(1000).inspect}" }
t2 = Thread.new { puts "#{1000} - #{trial(1000).inspect}" }
#t3 = Thread.new { puts "#{1000} - #{trial(1000).inspect}" }

t1.join
t2.join
#t3.join

t1 = Thread.new { puts "#{10000} - #{trial(10000).inspect}" }
t2 = Thread.new { puts "#{10000} - #{trial(10000).inspect}" }
#t3 = Thread.new { puts "#{10000} - #{trial(10000).inspect}" }

t1.join
t2.join
#t3.join