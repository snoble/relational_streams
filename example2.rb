require './relational_stream'
require './relational_event'
require 'redis'

redis = Redis.new

users = InputRStream.new("users", [:user])
charges = InputRStream.new("charges", [:user])

charges_with_cumulative_amount = charges.scan(0, redis, 'rr10') {|amount, c| amount + c[:amount]}
  .map {|c| {:user => c[:event][:user], :amount => c[:event][:amount], :cumulative_amount => c[:accumulator]}}

over_original_threshold = users.join(charges_with_cumulative_amount, redis, 'join11')
  .map {|x| {:user => x[:left][:user], :threshold => x[:left][:threshold], :amount => x[:right][:cumulative_amount]}}
  .select {|m| m[:amount] >= m[:threshold]}
  .select_first(redis, 'rr13')

# First version

over_original_threshold.each! {|m| puts "Create manual review for #{m[:user]} over original threshold"}

# Revised version

first_five_charges = charges
  .scan([], redis, 'rr11') {|charges, c| charges + [c]}
  .map {|c| {:user => c[:event][:user], :charges => c[:accumulator], :milestone => 'first_five_charges'}}
  .select_until(redis, 'rr12') {|c| c[:charges].size == 5}
  .select {|c| c[:charges].size == 5}

first_milestone = first_five_charges
  .concat(over_original_threshold.map {|c| c.merge({:milestone => 'over_original_threshold'})})
  .select_first(redis, 'rr14')

over_original_threshold_first = first_milestone
  .select {|milestone| milestone[:milestone] == 'over_original_threshold'}

threshold_update_first = first_milestone
  .select {|milestone| milestone[:milestone] == 'first_five_charges'}
  .map {|x| {:user => x[:user], :threshold => 1000}}
  .each! {|m| puts "Update threshold for #{m[:user]}"} 
  .join(charges_with_cumulative_amount, redis, 'join2')
  .map {|x| {:user => x[:left][:user], :threshold => x[:left][:threshold], :amount => x[:right][:cumulative_amount]}} 
  .select {|m| m[:amount] >= m[:threshold]}
  .select_first(redis, 'rr15')

create_manual_review = over_original_threshold_first.concat(threshold_update_first)
  .select_first(redis, 'rr16')

create_manual_review
  .each! {|m| puts "Create manual review for #{m[:user]} with threshold #{m[:threshold]} at #{m[:amount]}"}

require "json"
puts JSON.generate(create_manual_review.to_h, :max_nesting => 100)

# users.push({:user => 1, :threshold => 120})
# users.push({:user => 2, :threshold => 130})

# 7.times do |n|
#   charges.push({:user => 1, :amount => 50})
# end

# 150.times do |n|
#   charges.push({:user => 2, :amount => 15})
# end
