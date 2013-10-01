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
end
