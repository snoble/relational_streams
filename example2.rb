require './relational_stream'
require './relational_event'
require 'redis'

redis = Redis.new

merchants = EchoRStream.new([:merchant])
charges = EchoRStream.new([:merchant])

charges_with_cumulative_amount = charges.rolling_reduce(0, redis, 'rr0') {|amount, c| amount + c[:amount]}
  .map {|c| {:merchant => c[:event][:merchant], :amount => c[:event][:amount], :cumulative_amount => c[:accumulator]}}

first_five_charges = charges
  .rolling_reduce([], redis, 'rr1') {|charges, c| charges + [c]}
  .map {|c| {:merchant => c[:event][:merchant], :charges => c[:accumulator], :first_five_charges => true}}
  .select_until(redis, 'rr2') {|c| c[:charges].size == 5}
  .select {|c| c[:charges].size == 5}

over_original_threshold = merchants.join(charges_with_cumulative_amount, redis, 'join1')
  .map {|x| {:merchant => x[:left][:merchant], :threshold => x[:left][:threshold], :amount => x[:right][:cumulative_amount], :first_five_charges => false}}
  .select {|m| m[:amount] >= m[:threshold]}
  .select_first(redis, 'rr3')

first_milestone = first_five_charges
  .concat(over_original_threshold)
  .select_first(redis, 'rr4')

first_milestone
  .select {|milestone| !milestone[:first_five_charges]}
  .each! {|m| puts "Create manual review for #{m[:merchant]} (1)"}

first_milestone
  .select {|milestone| milestone[:first_five_charges]}
  .map {|x| {:merchant => x[:merchant], :threshold => 1000}}
  .each! {|m| puts "Update threshold for #{m[:merchant]}"} 
  .join(charges_with_cumulative_amount, redis, 'join2')
  .map {|x| {:merchant => x[:left][:merchant], :threshold => x[:left][:threshold], :amount => x[:right][:cumulative_amount]}} 
  .select {|m| m[:amount] >= m[:threshold]}
  .select_first(redis, 'rr5')
  .each! {|m| puts "Create manual review for #{m[:merchant]} (2)"}

merchants.push RelationalEvent.new({:merchant => 1, :threshold => 120}, {:merchant => 1})
merchants.push RelationalEvent.new({:merchant => 2, :threshold => 130}, {:merchant => 2})

7.times do |n|
  charges.push(RelationalEvent.new({:merchant => 1, :amount => 50}, {:merchant => 1}))
end

150.times do |n|
  charges.push(RelationalEvent.new({:merchant => 2, :amount => 15}, {:merchant => 2}))
end
