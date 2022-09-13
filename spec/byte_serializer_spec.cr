require "./spec_helper"

def rand_string(size)
  Array.new(size) { rand(32...126).unsafe_as(Char) }
    .join " "
end


module TestModule(T)
  getter name
  getter age
  getter points
  getter pets
  getter non_serializable_field

  macro included
    @[Field(ignore: true)]
    getter non_serializable_field : Bool = false

    def self.gen_random
      T.new \
        rand_string(10),
        rand(Int8),
        rand(Int32),
        Array.new 10 { rand_string 10 },
        non_serializable_field: true
    end
  end

  def initialize(@name : String, @age : Int8, @points : Int32, @pets : Array(String), @non_serializable_field=false)
  end

  def ==(other : T)
    @name == other.name && 
      @age == other.age && 
      @points == other.points &&
      @pets == other.pets
  end
end

struct TestStruct
  include Serializable
  include TestModule(TestStruct)
end

class TestClass
  include Serializable
  include TestModule(TestClass)
end


describe Serializable do
  it "String #to_io #from_io" do
    value = "test"
    io = IO::Memory.new
    value.to_io io
    String.from_io(io.rewind).should eq value
  end

  it "Array(Float) #to_io #from_io" do
    array = Array(Float32).new 10 { rand(Float32::MAX) }
    io = IO::Memory.new
    array.to_io io
    Array(Float32).from_io(io.rewind).should eq array
  end

  it "Array(String) #to_io #from_io" do
    array = Array(String).new 10 { rand_string 100 }
    io = IO::Memory.new
    array.to_io io
    Array(String).from_io(io.rewind).should eq array
  end

  it "Struct TestStruct #to_io #from_io" do
    value = TestStruct.gen_random
    io = IO::Memory.new
    value.to_io io
    value_from = TestStruct.from_io(io.rewind)
    value_from.should eq value
    value_from.non_serializable_field.should be_falsey
  end

  it "Class TestClass #to_io #from_io" do
    value = TestClass.gen_random
    io = IO::Memory.new
    value.to_io io
    value_from = TestClass.from_io(io.rewind)
    value_from.should eq value
    value_from.non_serializable_field.should be_falsey
  end
end
