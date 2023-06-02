module CryStorage
  annotation RefClass; end
    
  struct DataType(T)
    alias All = Bool | UInt8 | Int16 | Int32 | Int64 | Int128 | String
    alias Any = DataType(Bool) | DataType(UInt8) | DataType(Int16) | DataType(Int32) | DataType(Int64) | DataType(Int128) | DataType(String)
    
    Boolean = DataType(Bool).new 1
    Byte = DataType(UInt8).new 2
    SmallInt = DataType(Int16).new 10
    Integer = DataType(Int32).new 11
    BigInt = DataType(Int64).new 12
    HugeInt = DataType(Int128).new 13
    Text = DataType(String).new 50
    
    getter value

    def initialize(@value : Int32)
    end

    def ref_class
      T
    end

    def self.from_io(io, format)
      self.from io.read_bytes Int32
    end

    def self.from(value)
      case value
      when 1
        Bool
      when 2
        Char
      when 10
        SmallInt
      when 11
        Integer
      when 12
        BigInt
      when 13
        HugeInt
      when 50
        Text
      end
    end

    def to_io(io, format)
      io.write_bytes @value
    end

    def inspect
      to_s
    end

    def to_s
      "<DataType v:#{value}>"
    end
  end

  struct TableSchema
    @schema : String
    @name : String
    @bools_count : Int32?
    getter columns : Slice(Column)

    def initialize(@schema, @name, @columns)
      validate
      @columns.sort! &.order
    end

    def validate
      # raise "not implemented"
    end

    def bools
      @columns.each do |column|
        yield column if column.bool?
      end
    end

    def bools_count
      @bools_count ||= bools_count_compute
    end

    def bools_count_compute
      @columns.count &.bool?
    end

    def column(name)
      @columns.find &.name.== name
    end
  end

  struct Column
    enum Key
      PrimaryKey
    end

    getter name : String
    getter data_type : DataType::Any
    getter default : Bytes?
    getter order : Int32
    @is_nilable : Bool
    @key : Key?

    def initialize(@name, @data_type, @default, @order, @is_nilable, @key)
    end

    def nilable?
      @is_nilable
    end

    def bool?
      data_type == DataType::Boolean
    end
  end

  # struct Cell(T)
  #   property value : T

  #   def initialize(@value : T)
  #   end

  #   def self.from_io(io, type) : Cell
  #     Cell.new io.read_bytes type
  #   end

  #   def to_io(io, format)
  #     io.write_bytes @value
  #   end

  #   # FORWARD OPERATORS
  #   {% begin %}
  #     {% for operator in %w(== != <=> + - * / % ^ & | << >> ~) %}
  #       def {{operator.id}}(other); value {{operator.id}} other.value; end
  #     {% end %}
  #   {% end %}
  # end
end
