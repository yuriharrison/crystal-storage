macro not_nil(value, message = nil)
  def {{value}}!
    @{{value}}.not_nil! {{message}}
  end
end

macro not_nil_property(value, message)
  property {{value}}
  not_nil {{value}}, {{message}}
end
