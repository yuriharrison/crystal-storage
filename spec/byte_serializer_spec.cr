require "./spec_helper"

describe Serializer do
  it "#to_io" do
    io = IO::Memory.new
    value = "test"
    value.to_io io
  end
end
