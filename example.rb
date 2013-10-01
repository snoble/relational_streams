require './relational_stream'
require './relational_event'

x = EchoRStream.new([:a])
x.each! {|n| puts n[:a]}

y = x.select {|n| n[:a].even?}
y.each! {|n| puts n[:a] * 3}

another_x = EchoRStream.new([:a])

j = x.join(another_x)

jj = j.select_first
j.each! {|nn| puts "joined #{nn[:right][:b]}"}
jj.each! {|nn| puts "first joined #{nn[:right][:b]}"}

x.push(RelationalEvent.new({:a => 2}, {:a => 2}))
 .push(RelationalEvent.new({:a => 3}, {:a => 3}))

another_x
  .push(RelationalEvent.new({:a => 3, :b => 3}, {:a => 3}))
  .push(RelationalEvent.new({:a => 2, :b => 2}, {:a => 2}))
  .push(RelationalEvent.new({:a => 3, :b => 33}, {:a => 3}))
  .push(RelationalEvent.new({:a => 4, :b => 4}, {:a => 4}))

x.push(RelationalEvent.new({:a => 4}, {:a => 4}))
