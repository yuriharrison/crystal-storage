require "./utils"
require "./slot"

module CryStorage::PageManagement
  alias Link = Tuple(Int64, Int64)

  INT_SIZE = sizeof(Int64).to_i64

  abstract class IPage
  end

  abstract class IManager
    include Indexable::Mutable(IO::Memory)
  end

  abstract class IPage
    include Indexable::Mutable(ISlot)
    
    abstract def table
  end
  
  struct PageHeader
    SIZE = INT_SIZE*2

    property version
    property size

    def initialize(@version : Int64, @size : Int64)
    end

    def initialize(io : IO::Memory)
      initialize io.read_bytes(Int64), io.read_bytes(Int64)
    end
    
    def to_io(io, format)
      io.write_bytes @version
      io.write_bytes @size
    end
  end

  class Page(T) < IPage
    # TODO: implement transactions
    # TODO: make write operations thread-safe
    include Indexable::Item(IO::Memory)

    BYTE_SIZE = 1024_i64
    HEADER_SIZE = PageHeader::SIZE
    BODY_SIZE = BYTE_SIZE - HEADER_SIZE
    SLOT_LINK_SIZE = INT_SIZE*2

    @body : IO::Memory
    property table : Table

    def initialize(@manager : IManager, @id : Index, @table : Table, @buffer : IO::Memory)
      @header = PageHeader.new buffer
      @body = buffer.slice
    end

    def to_io(io, format)
      io.write_bytes @header
      io.copy_from @body
    end

    def indexer
      @manager
    end

    def index
      @id
    end

    def size
      @header.size
    end
    
    def unsafe_fetch(index : Int) : ISlot
      offset, content_size = slot_link index
      T.new self, index.to_i64, @body[offset, content_size]
    end

    def []=(slot_id : Int, slot : ISlot)
      not_full! slot
      unsafe_put slot_id, slot
    end

    def unsafe_put(index : Int, value : ISlot)
      offset, previous_size = slot_link index
      offset = next_offset value.byte_size if previous_size < value.byte_size
      slot_link_slice(index).write_bytes({ offset, value.byte_size })
      @body[offset, value.byte_size].write_bytes value
    end
    
    def push(slot : ISlot)
      not_full! slot.byte_size
      unsafe_push slot
    end

    def unsafe_push(slot : ISlot)
      slot.id = new_index
      slot.page = self
      unsafe_put(slot.id!, slot).tap { increase_slot_count }
    end
    
    def slot_cost(slot_byte_size)
      SLOT_LINK_SIZE + slot_byte_size
    end

    def full?(slot_byte_size)
      space_left < slot_cost slot_byte_size
    end

    def full?(slot : ISlot)
      if slot.page == self
        offset, previous_size = slot_link slot.id!
        return false if slot.byte_size <= previous_size
      end
      full? slot.byte_size
    end
    
    def last_index
      size - 1
    end

    def not_full!(slot_byte_size)
      raise "Page is full" if full? slot_byte_size
    end

    def to_s
      @header.to_s
    end

    private def space_left
      # BODY[ ...next_slot_link <SPACE_LEFT> next_slot_offset... ]
      next_offset(0) - size*SLOT_LINK_SIZE
    end

    private def next_offset(next_slot_size)
      if last_index < 0
        previous_offset = BODY_SIZE
      else
        previous_offset, _ = slot_link(last_index)
      end
      previous_offset - next_slot_size
    end

    private def new_index
      size
    end

    private def increase_slot_count
      @header.size += 1
    end

    private def slot_link(slot_id)
      io = slot_link_slice slot_id
      {io.read_bytes(Int64), io.read_bytes(Int64)} 
    end

    private def slot_link_slice(slot_id)
      @body[SLOT_LINK_SIZE*slot_id, SLOT_LINK_SIZE]
    end
  end

  class MemoryManager < IManager
    MIN_SIZE = 4
    PAGE_SIZE = 1024
    
    def initialize
      @buffer = IO::Memory.new Slice.new PAGE_SIZE*MIN_SIZE, UInt8::MIN
    end

    def size : Int
      (@buffer.size / PAGE_SIZE).to_i64
    end

    def unsafe_fetch(index : Int) : IO::Memory
      unsafe_fetch index.to_i64
    end

    def unsafe_fetch(index : Int64) : IO::Memory
      @buffer[index*PAGE_SIZE, PAGE_SIZE]
    end
    
    def unsafe_put(index : Int, value : IO::Memory)
      unsafe_fetch(index).write value.to_slice
    end
    
    def [](index : Int) : IO::Memory
      @buffer[index*PAGE_SIZE, PAGE_SIZE].copy.rewind
    end

    def []=(index : Int, page : IPage)
      unsafe_fetch(index).write_bytes page
    end

    def []=(index : Int, page : IPage)
      unsafe_fetch(index).write_bytes page
    end
  end
end
