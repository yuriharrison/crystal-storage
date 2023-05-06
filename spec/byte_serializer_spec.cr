require "./spec_helper"

def rand_string(size)
  Array.new(size) { rand(32...126).unsafe_as(Char) }
    .join " "
end


def test_to_from_io(value)
  IO::Memory.new
    .write_bytes(value)
    .rewind
    .read_bytes(typeof(value))
    .tap { should eq value }
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

enum TestEnum; One; Two end

describe Serializable do
  describe Bool do
    it "#to_io #from_io" do
      test_to_from_io true
      test_to_from_io false
    end
  end

  describe Enum do
    it "#to_io #from_io" do
      test_to_from_io TestEnum::One
      test_to_from_io TestEnum::Two
    end
  end

    describe String do
    it "#to_io #from_io" do
      test_to_from_io rand_string(10)
    end
  end

  describe Array do
    it "#to_io #from_io" do
      array = Array(Float32).new 10 { rand(Float32::MAX) }
      test_to_from_io array
    end
  end
  
  describe Array do
    it "#to_io #from_io" do
      array = Array(String).new 10 { rand_string 100 }
      test_to_from_io array
    end
  end

  describe Struct do
    it "#to_io #from_io" do
      test_to_from_io TestStruct.gen_random
    end
  end

  describe Class do
    it "#to_io #from_io" do
      value = test_to_from_io TestClass.gen_random
      value.non_serializable_field.should be_falsey
    end
  end
end


