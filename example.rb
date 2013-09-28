require './relational_stream'
require './relational_event'

x = EchoRStream.new
x.each! {|n| puts n[:a]}

y = x.select {|n| n[:a].even?}
y.each! {|n| puts n[:a] * 3}

another_x = EchoRStream.new

j = x.join(another_x, [:a])
  .map do |n|
    event = RelationalEvent.new(:left => n[:left], :right => n[:right], :a => n[:left][:a])
  end

jj = j.select_first([:a])
j.each! {|nn| puts "joined #{nn[:right][:b]}"}
jj.each! {|nn| puts "first joined #{nn[:right][:b]}"}

x.push(RelationalEvent.new(:a => 2))
 .push(RelationalEvent.new(:a => 3))

another_x
  .push(RelationalEvent.new(:a => 3, :b => 3))
  .push(RelationalEvent.new(:a => 2, :b => 2))
  .push(RelationalEvent.new(:a => 3, :b => 33))
  .push(RelationalEvent.new(:a => 4, :b => 4))

x.push(RelationalEvent.new(:a => 4))
