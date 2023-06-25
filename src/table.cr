module CryStorage
  annotation RefClass; end

  module Table
    abstract def schema
    abstract def get(address : Address) : PageManagement::ISlot
    abstract def insert(slot : ISlot)
    abstract def scan(& : ISlot ->)
    abstract def indexer(column : Column, range=false)
  end
    
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
    getter schema : String
    getter name : String
    @bools_count : Int32?
    getter columns : Slice(Column)

    def initialize(@schema, @name, @columns, original=false)
      validate
      return if original
      @columns.sort! &.order
      @columns.each { |col| col.table = self }
    end

    def ==(other)
      !other.nil? && name == other.name && schema == other.schema
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
    property table : TableSchema?
    @is_nilable : Bool
    @key : Key?

    def initialize(@name, @data_type, @default, @order, @is_nilable, @key)
    end

    def nilable?
      @is_nilable
    end

    def ==(other)
      name == other.name && (table == other.table)
    end

    def bool?
      @data_type == DataType::Boolean
    end

    def to_s(io)
      io << '`'
      io << @table.not_nil!.name unless @table.nil?
      io << '.' << @name << '`'
    end
  end

  class TableMeta
    getter id : Index
    setter space_left : Int32

    def initialize(@id, @space_left)
    end

    def fits?(slot : ISlot)
      !full?(slot.byte_size)
    end

    def full?(size)
      @space_left < size
    end
  end

  class RowTable
    include Table
    # TODO implement; diff tmp table from actual table
    # TODO implement; implement serialization
    getter schema : TableSchema
    @indices : Array(Indexers::Indexer)?
    @pages : Array(TableMeta)
    @pageManager : PageManagement::IManager

    def initialize(@schema, @pageManager = PageManagement::MemoryManager.default)
      initialize(@schema, @pageManager, nil)
    end

    def initialize(@schema, @pageManager, @indices)
      # TODO make this a heap
      @pages = Array(TableMeta).new
    end

    def self.from(left_table, right_table)
      from "join", "00001", left_table.schema.columns + right_table.schema.columns
    end

    def self.from(schema, name, columns)
      new TableSchema.new(schema, name, columns)
    end

    def get(address : Address) : PageManagement::ISlot
      raise "not implemented"
    end

    def scan
      @pages.each do |meta|
        @pageManager.table_page(meta.id, schema).each do |slot|
          yield slot
        end
      end
    end

    def insert(slot)
      page = next_page(slot) || new_page
      page.push slot
      # TODO update page heap meta
    end

    def insert(left_slot, right_slot)
      insert Slot.new(schema, left_slot.values + right_slot.values)
    end

    def indexer(column : Column, range=false)
      return nil if @indices.nil?

      @indices.not_nil!.find { |indexer|
        indexer.columns.any?(&.==(column)) &&
        (!range || indexer.range?)
      }
    end

    protected def next_page(slot : ISlot)
      meta = @pages.find &.fits? slot
      return nil unless meta
      @pageManager.table_page meta.id, schema
    end

    protected def new_page
      @pageManager.new_table_page schema
    end
  end

end
