require './relational_stream'
require './relational_event'
require 'redis'

redis = Redis.new

x = InputRStream.new("x", [:a])
x.each! {|n| puts "#{n[:a]} added to x"}

y = x.select {|n| n[:a].even?}
y.each! {|n| puts "#{n[:a]} filtered to y"}

another_x = InputRStream.new("another_x", [:a])

j = x.join(another_x, redis, 'join')

jj = j.select_first(redis, 'select first')
j.each! {|nn| puts "joined #{nn[:right][:b]} from x and another_x on key #{nn[:right][:a]}"}
jj.each! {|nn| puts "first join of key #{nn[:right][:a]} from x and another_x"}

x.push({:a => 2})
 .push({:a => 3})

another_x
  .push({:a => 3, :b => 3})
  .push({:a => 2, :b => 2})
  .push({:a => 3, :b => 33})
  .push({:a => 4, :b => 4})

x.push({:a => 4})
