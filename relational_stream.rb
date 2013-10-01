require './relational_event'

class RelationalStream
  attr_accessor :each_bang_methods, :subscribers, :subscriptions, :active, :keys, :keys_match_subscriptions, :each_methods

  def initialize(keys)
    @keys = keys
    @each_bang_methods = []
    @each_methods = []
    @subscribers = []
    @subscriptions = []
    @active = false
    @mutes = {}
    @keys_match_subscriptions = true
  end

  def subscribe_to(other, opts = {})
    other.subscribers << RelationalStreamSubscription.new(self, opts)
    @subscriptions << other
  end

  def pass_up(&block)
    subscriptions.each {|s| block.yield(s)}
  end

  def activate!
    unless @active
      @active = true
      pass_up {|s| s.activate!}
    end
  end

  def make_mute(event)
    mute = keys.reduce(@mutes) do |hash, key|
      hash[event.key(key)] = {} unless hash.member?(event.key(key))
      hash[event.key(key)]
    end
    mute
  end

  def active_for_event?(event)
    mute = keys.reduce(@mutes) do |hash, key|
      return true unless hash.member?(event.key(key))
      hash[event.key(key)]
    end
    false
  end

  def mute_event_by_key(event)
    pass_up {|s| s.mute_event_by_key(event)} if keys_match_subscriptions
    make_mute(event)
    self
  end

  def each!(&block)
    activate!
    @each_bang_methods << block
    self
  end

  def each(&block)
    @each_methods << block
    self
  end

  def emit(event)
    if @active and active_for_event?(event)
      @subscribers.each {|s| s.subscriber.push(event, s.options)}
      @each_bang_methods.each {|m| m.yield(event)}
      @each_methods.each {|m| m.yield(event)}
    end
  end

  def rekey(new_keys)
    rekey_stream = EchoRStream.new(new_keys)
    rekey_stream.subscribe_to(self)
    rekey_stream.keys_match_subscriptions = false
    rekey_stream
  end

  def flatmap(&block)
    flatmap_stream = FlatmapRStream.new(keys)
    flatmap_stream.map_proc = block
    flatmap_stream.subscribe_to(self)
    flatmap_stream
  end

  def map(&block)
    flatmap {|e| [block.yield(e)]}
  end

  def join(other_stream)
    join_stream = JoinRStream.new(keys)
    join_stream.subscribe_to(self, :side => :left)
    join_stream.subscribe_to(other_stream, :side => :right)
    join_stream
  end

  def select(&block)
    flatmap {|e| block.yield(e) ? [e] : []}
  end

  def concat(other_stream)
    concat_stream = EchoRStream.new(keys)
    concat_stream.subscribe_to(self)
    concat_stream.subscribe_to(other_stream)
    concat_stream
  end

  def rolling_reduce(init, &block)
    rolling_reduce_stream = RollingReduceRStream.new(keys)
    rolling_reduce_stream.initial = init
    rolling_reduce_stream.reduce_proc = block
    rolling_reduce_stream.subscribe_to(self)
    rolling_reduce_stream
  end

  def select_until(&block)
    rolling_reduce(0) do |acc, e|
      next 2 if acc > 0
      block.yield(e) ? 1 : 0
    end
    .each {|x| make_mute(x) if x[:accumulator] >= 1}
    .select {|x| x[:accumulator] < 2}
    .map {|x| x[:event].dict}
  end

  def select_first
    select_until {|event| true}
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
    @map_proc.yield(event).each {|x| emit(RelationalEvent.new(x, event.keys))}
    self
  end
end

class JoinRStream < RelationalStream
  attr_accessor :value_store

  def initialize(keys)
    super(keys)
    @value_store = {}
  end

  def find_or_make_sides(event)
    sides = keys.reduce(@value_store) do |hash, key|
      hash[event.key(key)] = {} unless hash.member?(event.key(key))
      hash[event.key(key)]
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
      emit RelationalEvent.new({side => event, other_side => other_event}, event.keys)
    end
    self
  end
end

class RollingReduceRStream < RelationalStream
  attr_accessor :value_store, :initial, :reduce_proc
  def initialize(keys)
    super(keys)
    @value_store = {}
  end

  def find_or_make_accumulator(event)
    accumulator = keys.reduce(@value_store) do |hash, key|
      hash[event.key(key)] = {} unless hash.member?(event.key(key))
      hash[event.key(key)]
    end
    accumulator[:value] = initial unless accumulator.member?(:value)
    accumulator
  end

  def push(event, opts = {})
    accumulator = find_or_make_accumulator(event)
    accumulator[:value] = reduce_proc.yield(accumulator[:value], event)
    emit RelationalEvent.new({:event => event, :accumulator => accumulator[:value]}, event.keys)
    self
  end
end
