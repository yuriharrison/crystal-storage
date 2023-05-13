require "./extentions"

module CryStorage::PageManagement
  @[Flags]
  enum SlotStatus
    Locked
    Deleted
  end

  abstract class ISlot
    # TODO: convert interfaces in modules
    include Indexable::Item(ISlot)

    abstract def byte_size
    abstract def delete
    abstract def deleted?

    def address : Address
      { indexer.index, index }
    end
  end

  # TODO: index slot
  # TODO: Write table slot, must support column types Integer Varchar and Boolean
  
  annotation RefClass; end
  
  enum DataType
    @[RefClass(Bool)]
    Bool = 1
    @[RefClass(Char)]
    Char = 2
    @[RefClass(Int16)]
    Int16 = 10
    @[RefClass(Int32)]
    Int32 = 11
    @[RefClass(Int64)]
    Int64 = 12
    @[RefClass(Int64)]
    Int = 13
    @[RefClass(Int128)]
    Int128 = 14
    @[RefClass(String)]
    Text = 50
    
    def ref_class
      {% unless self.annotation(RefClass) %}
        raise "DataType: #{value} must implement annotation RefClass."
      {% end %}
      {{ self.annotation(RefClass)[0] }}
    end
  end

  struct Table
    @schema : String
    @name : String
    getter columns : Slice(Column)
    @columns_map : Hash(String, Column)

    def initialize(@schema, @name, @columns)
      # sort wont work with UInt64
      # @columns.sort! &.order
      @columns_map = @columns.to_h { |c| { c.name, c } }
    end

    def bools
      @columns.each do |column|
        yield column if column.data_type == DataType::Bool
      end
    end

    def column(name)
      @columns_map[name]
    end
  end

  struct Column
    enum Key
      PrimaryKey
    end

    getter name : String
    getter data_type : DataType
    getter default : Bytes?
    getter order : UInt64
    @is_nilable : Bool
    @key : Key?

    def initialize(@name, @data_type, @default, @order, @is_nilable, @key)
    end

    def nilable?
      @is_nilable
    end
  end

  struct Cell(T)
    property value : T

    def initialize(@value : T)
    end

    def self.from_io(io, type) : Cell
      Cell.new io.read_bytes type
    end

    def to_io(io, format)
      io.write_bytes @value
    end
  end

  class Slot < ISlot
    # TODO implement self.from_io
    # TODO fix warnnings
    property status : SlotStatus = SlotStatus.from_value(0)
    property page : Page(TestSlot)? = nil
    not_nil page
    property id : Int64? = nil
    not_nil id
    property table : Table?
    not_nil table
      
    def initialize(@page : IPage, @id : Index, io : IO::Memory)
      @status = io.read_bytes SlotStatus
      # TODO fix this
      # implement self.from_io
      # modify Page slot retrival use from_io
      # decide where table info should live; probably on page
      @io = io
    end

    def load
      @values = Slice.new table!.columns.size do |i|
        @io.read_bytes table!.columns[i].data_type.ref_class
      end
    end

    def get(column)
      index = table!.columns.index &.name == column
      @values[index]
    end

    def get(column : Column)
      get column.name
    end

    def indexer : IPage
      page!
    end
    
    def index
      id!
    end
  
    def delete
      @status |= SlotStatus::Deleted
    end
  
    def deleted?
      @status.deleted?
    end
  
    def byte_size
      # TODO: implement, add #byte_size to all serializable objects
      200
    end
  
    def to_io(io, format)
      io.write_bytes @status
      @values.each { |value| io.write_bytes value }
    end
  
    def to_s
      raise "not implemented"
    end

  end

end

# COLUMNS
# +--------------------------+---------------------+------+-----+---------+-------+
# | Field                    | Type                | Null | Key | Default | Extra |
# +--------------------------+---------------------+------+-----+---------+-------+
# | TABLE_CATALOG            | varchar(512)        | NO   |     | NULL    |       |
# | TABLE_SCHEMA             | varchar(64)         | NO   |     | NULL    |       |
# | TABLE_NAME               | varchar(64)         | NO   |     | NULL    |       |
# | COLUMN_NAME              | varchar(64)         | NO   |     | NULL    |       |
# | ORDINAL_POSITION         | bigint(21) unsigned | NO   |     | NULL    |       |
# | COLUMN_DEFAULT           | longtext            | YES  |     | NULL    |       |
# | IS_NULLABLE              | varchar(3)          | NO   |     | NULL    |       |
# | DATA_TYPE                | varchar(64)         | NO   |     | NULL    |       |
# | CHARACTER_MAXIMUM_LENGTH | bigint(21) unsigned | YES  |     | NULL    |       |
# | CHARACTER_OCTET_LENGTH   | bigint(21) unsigned | YES  |     | NULL    |       |
# | NUMERIC_PRECISION        | bigint(21) unsigned | YES  |     | NULL    |       |
# | NUMERIC_SCALE            | bigint(21) unsigned | YES  |     | NULL    |       |
# | DATETIME_PRECISION       | bigint(21) unsigned | YES  |     | NULL    |       |
# | CHARACTER_SET_NAME       | varchar(32)         | YES  |     | NULL    |       |
# | COLLATION_NAME           | varchar(64)         | YES  |     | NULL    |       |
# | COLUMN_TYPE              | longtext            | NO   |     | NULL    |       |
# | COLUMN_KEY               | varchar(3)          | NO   |     | NULL    |       |
# | EXTRA                    | varchar(80)         | NO   |     | NULL    |       |
# | PRIVILEGES               | varchar(80)         | NO   |     | NULL    |       |
# | COLUMN_COMMENT           | varchar(1024)       | NO   |     | NULL    |       |
# | IS_GENERATED             | varchar(6)          | NO   |     | NULL    |       |
# | GENERATION_EXPRESSION    | longtext            | YES  |     | NULL    |       |
# +--------------------------+---------------------+------+-----+---------+-------+

# TABLES
# +------------------+---------------------+------+-----+---------+-------+
# | Field            | Type                | Null | Key | Default | Extra |
# +------------------+---------------------+------+-----+---------+-------+
# | TABLE_CATALOG    | varchar(512)        | NO   |     | NULL    |       |
# | TABLE_SCHEMA     | varchar(64)         | NO   |     | NULL    |       |
# | TABLE_NAME       | varchar(64)         | NO   |     | NULL    |       |
# | TABLE_TYPE       | varchar(64)         | NO   |     | NULL    |       |
# | ENGINE           | varchar(64)         | YES  |     | NULL    |       |
# | VERSION          | bigint(21) unsigned | YES  |     | NULL    |       |
# | ROW_FORMAT       | varchar(10)         | YES  |     | NULL    |       |
# | TABLE_ROWS       | bigint(21) unsigned | YES  |     | NULL    |       |
# | AVG_ROW_LENGTH   | bigint(21) unsigned | YES  |     | NULL    |       |
# | DATA_LENGTH      | bigint(21) unsigned | YES  |     | NULL    |       |
# | MAX_DATA_LENGTH  | bigint(21) unsigned | YES  |     | NULL    |       |
# | INDEX_LENGTH     | bigint(21) unsigned | YES  |     | NULL    |       |
# | DATA_FREE        | bigint(21) unsigned | YES  |     | NULL    |       |
# | AUTO_INCREMENT   | bigint(21) unsigned | YES  |     | NULL    |       |
# | CREATE_TIME      | datetime            | YES  |     | NULL    |       |
# | UPDATE_TIME      | datetime            | YES  |     | NULL    |       |
# | CHECK_TIME       | datetime            | YES  |     | NULL    |       |
# | TABLE_COLLATION  | varchar(64)         | YES  |     | NULL    |       |
# | CHECKSUM         | bigint(21) unsigned | YES  |     | NULL    |       |
# | CREATE_OPTIONS   | varchar(2048)       | YES  |     | NULL    |       |
# | TABLE_COMMENT    | varchar(2048)       | NO   |     | NULL    |       |
# | MAX_INDEX_LENGTH | bigint(21) unsigned | YES  |     | NULL    |       |
# | TEMPORARY        | varchar(1)          | YES  |     | NULL    |       |
# +------------------+---------------------+------+-----+---------+-------+
