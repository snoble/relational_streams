class RelationalEvent
  attr_accessor :dict, :keys

  def initialize(dict, keys)
    @dict = dict
    @keys = keys
  end

  def key(x)
    @keys[x]
  end

  def [](x)
    dict[x]  
  end

  def to_hash
    dict.to_hash
  end

  def merge(x)
    RelationalEvent.new(x.merge(@dict), @keys)
  end

  def marshal_dump
    [@dict, @keys]
  end

  def marshal_load(array)
    @dict, @keys = array
  end
end
