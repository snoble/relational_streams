require 'redis'
require './relational_event'

class RelationalStream
  attr_accessor :each_bang_methods, :subscribers, :subscriptions, :active, :keys, :keys_match_subscriptions

  def initialize(keys)
    @keys = keys
    @each_bang_methods = []
    @subscribers = []
    @subscriptions = []
    @active = true
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

  def each!(&block)
    activate!
    @each_bang_methods << block
    self
  end

  def emit(event)
    if @active
      @subscribers.each {|s| s.subscriber.push(event, s.options)}
      @each_bang_methods.each {|m| m.yield(event)}
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

  def join(other_stream, redis, redis_key)
    join_stream = JoinRStream.new(keys, redis, redis_key)
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

  def scan(init, redis, redis_key, &block)
    scan_stream = ScanRStream.new(keys, redis, redis_key)
    scan_stream.initial = init
    scan_stream.reduce_proc = block
    scan_stream.subscribe_to(self)
    scan_stream
  end

  def select_until(redis, redis_key, &block)
    scan(0, redis, redis_key) do |acc, e|
      next 2 if acc > 0
      block.yield(e) ? 1 : 0
    end
    .select {|x| x[:accumulator] < 2}
    .map {|x| x[:event].dict}
  end

  def select_first(redis, redis_key)
    select_until(redis, redis_key) {|event| true}
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
  attr_accessor :redis, :redis_key

  def initialize(keys, redis, redis_key)
    super(keys)
    @redis = redis
    @redis_key = redis_key
  end

  def find_or_make_sides(event)
    sides = {}
    sides[:left] = redis_key + Marshal.dump(event.keys) + 'left'
    sides[:right] = redis_key + Marshal.dump(event.keys) + 'right'
    sides
  end

  def push(event, opts)
    side = opts[:side]
    other_side = side == :left ? :right : :left
    sides = find_or_make_sides(event)
    redis.lpush(sides[side], Marshal.dump(event))
    n = 0
    while !(x = redis.lindex(sides[other_side], n)).nil?
      # A while loop?! seriously?! what are we 12?
      other_event = Marshal.load(x)
      emit RelationalEvent.new({side => event, other_side => other_event}, event.keys)
      n += 1
    end
    self
  end
end

class ScanRStream < RelationalStream
  attr_accessor :initial, :reduce_proc, :redis, :redis_key
  def initialize(keys, redis, redis_key)
    super(keys)
    @redis = redis
    @redis_key = redis_key
  end

  def find_or_make_accumulator(event)
    redis_key + Marshal.dump(event.keys)
  end

  def push(event, opts = {})
    accumulator = find_or_make_accumulator(event)
    x = redis.get(accumulator)
    prev_acc = x.nil? ? initial : Marshal.load(x)
    next_acc = reduce_proc.yield(prev_acc, event)
    redis.set(accumulator, Marshal.dump(next_acc))
    emit RelationalEvent.new({:event => event, :accumulator => next_acc}, event.keys)
    self
  end
end
