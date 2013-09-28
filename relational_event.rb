class RelationalEvent
  attr_accessor :dict

  def initialize(dict)
    @dict = dict
  end

  def [](x)
    dict[x]  
  end
end
