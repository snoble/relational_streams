require './relational_event'

class RelationalStream
  attr_accessor :each_bang_methods, :subscribers

  def initialize()
    @each_bang_methods = []
    @subscribers = []
  end

  def subscribe_to(other, opts = {})
    other.subscribers << RelationalStreamSubscription.new(self, opts)
  end

  def each!(&block)
    @each_bang_methods << block
    self
  end

  def emit(event)
    @subscribers.each {|s| s.subscriber.push(event, s.options)}
    @each_bang_methods.each {|m| m.yield(event)}
  end

  def flatmap(&block)
    flatmap_stream = FlatmapRStream.new
    flatmap_stream.map_proc = block
    flatmap_stream.subscribe_to(self)
    flatmap_stream
  end

  def map(&block)
    flatmap {|e| [block.yield(e)]}
  end

  def join(other_stream, keys)
    join_stream = JoinRStream.new
    join_stream.keys = keys
    join_stream.subscribe_to(self, :side => :left)
    join_stream.subscribe_to(other_stream, :side => :right)
    join_stream
  end

  def select(&block)
    flatmap {|e| block.yield(e) ? [e] : []}
  end

  def concat(other_stream)
    concat_stream = EchoRStream.new
    concat_stream.subscribe_to(self)
    concat_stream.subscribe_to(other_stream)
    concat_stream
  end

  def rolling_reduce(init, keys, &block)
    rolling_reduce_stream = RollingReduceRStream.new
    rolling_reduce_stream.initial = init
    rolling_reduce_stream.keys = keys
    rolling_reduce_stream.reduce_proc = block
    rolling_reduce_stream.subscribe_to(self)
    rolling_reduce_stream
  end

  def select_until(keys, &block)
    rolling_reduce(0, keys) do |acc, e|
      next 2 if acc > 0
      block.yield(e) ? 1 : 0
    end
    .select {|x| 
      x[:accumulator] < 2}
    .map {|x| x[:event]}
  end

  def select_first(keys)
     select_until(keys) {|event| true}
  end

end

class RelationalStreamSubscription
  attr_accessor :subscriber, :options
  def initialize(s, o = {})
    @subscriber = s
    @options = o
  end
end

class EchoRStream < RelationalStream
  def push(event, opts = {})
    emit(event)
    self
  end
end

class FlatmapRStream < RelationalStream
  attr_accessor :map_proc

  def push(event, opts = {})
    @map_proc.yield(event).each {|x| emit(x)}
    self
  end
end

class JoinRStream < RelationalStream
  attr_accessor :keys, :value_store

  def initialize
    super
    @value_store = {}
  end

  def find_or_make_sides(event)
    sides = keys.reduce(@value_store) do |hash, key|
      hash[event[key]] = {} unless hash.member?(event[key])
      hash[event[key]]
    end
    sides[:left] = [] unless sides.member?(:left)
    sides[:right] = [] unless sides.member?(:right)
    sides
  end

  def push(event, opts)
    side = opts[:side]
    other_side = side == :left ? :right : :left
    sides = find_or_make_sides(event)
    sides[side] << event
    sides[other_side].each do |other_event|
      emit RelationalEvent.new(side => event, other_side => other_event)
    end
    self
  end
end

class RollingReduceRStream < RelationalStream
  attr_accessor :value_store, :initial, :reduce_proc, :keys
  def initialize
    super
    @value_store = {}
  end

  def find_or_make_accumulator(event)
    accumulator = keys.reduce(@value_store) do |hash, key|
      hash[event[key]] = {} unless hash.member?(event[key])
      hash[event[key]]
    end
    accumulator[:value] = initial unless accumulator.member?(:value)
    accumulator
  end

  def push(event, opts = {})
    accumulator = find_or_make_accumulator(event)
    accumulator[:value] = reduce_proc.yield(accumulator[:value], event)
    emit RelationalEvent.new(:event => event, :accumulator => accumulator[:value])
    self
  end
end
