require "bit_array"
require "./extentions"
require "./table"

module CryStorage::PageManagement
  @[Flags]
  enum SlotStatus
    Idle = 0
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

  class Slot < ISlot
    # TODO save bools separatly
    
    property status : SlotStatus = SlotStatus::Idle
    property page : IPage? = nil
    not_nil page
    property id : Int64? = nil
    not_nil id

    @table : Table
    @values : Slice(DataType::All)
    @nulls : BitArray
    @bools : BitArray
      
    def initialize(@table, io : IO::Memory)
      @status = io.read_bytes SlotStatus
      @nulls = io.read_bytes BitArray
      @bools = io.read_bytes BitArray
      @values = Slice.new @table.columns.size do |i|
        io.read_bytes @table.columns[i].data_type.ref_class
      end
    end

    def initialize(@page : IPage, @id : Index, io : IO::Memory)
      initialize page!.table, io
    end

    def self.from(table : Table, *args)
      Slot.new table, IO::Memory.build {
        write_bytes SlotStatus::Idle
        write_bytes BitArray.new 0
        write_bytes BitArray.new table.bools_count
        write_bytes args
      }
    end
    
    def to_io(io, format)
      io.write_bytes @status
      io.write_bytes @nulls
      io.write_bytes @bools
      @values.each { |value| io.write_bytes value }
    end

    def set(column, value)
      @values[column_index(column)] = value
    end
    
    def get(column_name)
      get @table.column column_name
    end
    
    def get(column : Column)
      @values[column_index(column.name)]
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
    
    
    def to_s
      raise "not implemented"
    end
    
    private def column_index(column)
      @table.columns.index! &.name.== column
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
