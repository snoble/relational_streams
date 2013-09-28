require './relational_stream'
require './relational_event'

merchants = EchoRStream.new
charges = EchoRStream.new

charges_with_cumulative_amount = charges.rolling_reduce(0, [:merchant]) {|amount, c| amount + c[:amount]}
  .map {|c| RelationalEvent.new(:merchant => c[:event][:merchant], :amount => c[:event][:amount], :cumulative_amount => c[:accumulator])}

first_five_charges = charges
  .rolling_reduce([], [:merchant]) {|charges, c| charges + [c]}
  .map {|c| RelationalEvent.new(:merchant => c[:event][:merchant], :charges => c[:accumulator], :first_five_charges => true)}
  .select_until([:merchant]) {|c| c[:charges].size == 5}
  .select {|c| c[:charges].size == 5}

over_original_threshold = merchants.join(charges_with_cumulative_amount, [:merchant])
  .map {|x| RelationalEvent.new(:merchant => x[:left][:merchant], :threshold => x[:left][:threshold], :amount => x[:right][:cumulative_amount], :first_five_charges => false)}
  .select {|m| m[:amount] >= m[:threshold]}
  .select_first([:merchant])

first_milestone = first_five_charges
  .concat(over_original_threshold)
  .select_first([:merchant])

first_milestone
  .select {|milestone| !milestone[:first_five_charges]}
  .each! {|m| puts "Create manual review for #{m[:merchant]} (1)"}

first_milestone
  .select {|milestone| milestone[:first_five_charges]}
  .map {|x| RelationalEvent.new(:merchant => x[:merchant], :threshold => 1000)}
  .each! {|m| puts "Update threshold for #{m[:merchant]}"} 
  .join(charges_with_cumulative_amount, [:merchant])
  .map {|x| RelationalEvent.new(:merchant => x[:left][:merchant], :threshold => x[:left][:threshold], :amount => x[:right][:cumulative_amount])} 
  .select {|m| m[:amount] >= m[:threshold]}
  .select_first([:merchant])
  .each! {|m| puts "Create manual review for #{m[:merchant]} (2)"}

merchants.push RelationalEvent.new(:merchant => 1, :threshold => 120)
merchants.push RelationalEvent.new(:merchant => 2, :threshold => 130)

7.times do |n|
  charges.push(RelationalEvent.new(:merchant => 1, :amount => 50))
end

150.times do |n|
  charges.push(RelationalEvent.new(:merchant => 2, :amount => 15))
end
