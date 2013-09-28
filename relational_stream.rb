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
  end

  def emit(event)
    @subscribers.each {|s| s.subscriber.push(event, s.options)}
    @each_bang_methods.each {|m| m.yield(event)}
  end

  def map(&block)
    map_stream = MapRStream.new
    map_stream.map_proc = block
    map_stream.subscribe_to(self)
    map_stream
  end

  def join(other_stream, keys)
    join_stream = JoinRStream.new
    join_stream.keys = keys
    join_stream.subscribe_to(self, :side => :left)
    join_stream.subscribe_to(other_stream, :side => :right)
    join_stream
  end

  def select(&block)
    select_stream = SelectRStream.new
    select_stream.filter_proc = block
    select_stream.subscribe_to(self)
    select_stream
  end

  def concat(other_stream)
    concat_stream = EchoRStream.new
    concat_stream.subscribe_to(self)
    concat_stream.subscribe_to(other_stream)
    concat_stream
  end

  def rolling_reduce(init, keys, &block)
    rolling_reduce_stream = Rol
    
  end

  def select_until(keys, &block)
    select_until_stream = SelectUntilRStream.new
    select_until_stream.keys = keys
    select_until_stream.filter_proc = block
    select_until_stream.subscribe_to(self)
    select_until_stream
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

class SelectRStream < RelationalStream
  attr_accessor :filter_proc

  def push(event, opts = {})
    emit(event) if @filter_proc.yield(event)
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

class MapRStream < RelationalStream
  attr_accessor :map_proc

  def push(event, opts = {})
    emit map_proc.yield(event)
    self
  end
end

class SelectUntilRStream < RelationalStream
  attr_accessor :value_store, :filter_proc, :keys

  def initialize
    super
    @value_store = {}
  end

  def find_or_make_filter(event)
    filter = keys.reduce(@value_store) do |hash, key|
      hash[event[key]] = {} unless hash.member?(event[key])
      hash[event[key]]
    end
    filter[:value] = false unless filter.member?(:value)
    filter
  end

  def push(event, opts = {})
    filter = find_or_make_filter(event)
    emit(event) unless filter[:value]
    filter[:value] ||= filter_proc.yield(event)
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
    accumulator[:value] = reduce_proc.yield(accumulator, event)
    emit RelationalEvent.new(:event => event, :accumulator => accumulator[:value])
    self
  end
end
