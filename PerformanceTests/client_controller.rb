require 'PerformanceTests\test_client'

class ClientController
  clients = Array.new
  5.times do
    clients << TestClient.new
  end
  
  results = Array.new
  num = 0
  clients.each do |client|
    results[num] = Thread.new{client.start("small")}.value
    num += 1
    
  end
  
  puts results
end