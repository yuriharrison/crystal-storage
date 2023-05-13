macro not_nil(value)
  def {{value}}!
    @{{value}}.not_nil!
  end
end
