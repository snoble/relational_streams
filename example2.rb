require './relational_stream'
require './relational_event'
require 'redis'

redis = Redis.new

users = EchoRStream.new([:user])
charges = EchoRStream.new([:user])

charges_with_cumulative_amount = charges.scan(0, redis, 'rr10') {|amount, c| amount + c[:amount]}
  .map {|c| {:user => c[:event][:user], :amount => c[:event][:amount], :cumulative_amount => c[:accumulator]}}

first_five_charges = charges
  .scan([], redis, 'rr11') {|charges, c| charges + [c]}
  .map {|c| {:user => c[:event][:user], :charges => c[:accumulator], :milestone => 'first_five_charges'}}
  .select_until(redis, 'rr12') {|c| c[:charges].size == 5}
  .select {|c| c[:charges].size == 5}

over_original_threshold = users.join(charges_with_cumulative_amount, redis, 'join11')
  .map {|x| {:user => x[:left][:user], :threshold => x[:left][:threshold], :amount => x[:right][:cumulative_amount], :milestone => 'over_original_threshold'}}
  .select {|m| m[:amount] >= m[:threshold]}
  .select_first(redis, 'rr13')

first_milestone = first_five_charges
  .concat(over_original_threshold)
  .select_first(redis, 'rr14')

first_milestone
  .select {|milestone| milestone[:milestone] == 'over_original_threshold'}
  .each! {|m| puts "Create manual review for #{m[:user]} (1)"}

first_milestone
  .select {|milestone| milestone[:milestone] == 'first_five_charges'}
  .map {|x| {:user => x[:user], :threshold => 1000}}
  .each! {|m| puts "Update threshold for #{m[:user]}"} 
  .join(charges_with_cumulative_amount, redis, 'join2')
  .map {|x| {:user => x[:left][:user], :threshold => x[:left][:threshold], :amount => x[:right][:cumulative_amount]}} 
  .select {|m| m[:amount] >= m[:threshold]}
  .select_first(redis, 'rr15')
  .each! {|m| puts "Create manual review for #{m[:user]} (2)"}

users.push RelationalEvent.new({:user => 1, :threshold => 120}, {:user => 1})
users.push RelationalEvent.new({:user => 2, :threshold => 130}, {:user => 2})

7.times do |n|
  charges.push(RelationalEvent.new({:user => 1, :amount => 50}, {:user => 1}))
end

150.times do |n|
  charges.push(RelationalEvent.new({:user => 2, :amount => 15}, {:user => 2}))
end
