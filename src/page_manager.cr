struct Tuple
  def to_io(io : IO)
    each { |n| io.write_bytes n }
  end
end

class IO::Memory
  def copy(io : IO)
    IO.copy self, io
    io
  end

  def copy_from(io : IO)
    io.copy self
  end
end

class IO::Memory
  def slice(offet, size) : Memory
    Memory.new to_slice[@pos + offet, size], writeable: @writeable
  end

  def slice
    slice 0, size - @pos
  end

  def [](offset, size)
    slice offset, size
  end

  def copy
    io = IO::Memory.new
    copy io
  end
end

module Indexable::Item(T)
  abstract def index : Int
  abstract def indexer : Indexable::Mutable(T)

  def flush
    indexer[index] = self
  end
end

module CryStorage::PageManagement
  alias Index = Int64
  alias Address = Tuple(Index, Index)
  alias Link = Tuple(Int64, Int64)

  INT_SIZE = sizeof(Int64)
  
  abstract struct ISlot
  end

  abstract struct IPage
  end

  abstract struct IManager
    include Indexable::Mutable(IO::Memory)
  end

  abstract struct IPage
    include Indexable::Mutable(ISlot)
    include Indexable::Item(IO::Memory)

    abstract def initialize(@manager : IManager, @id : Index, @buffer : IO::Memory)

    def indexer : IManager
      @manager
    end

    def index : Index
      @id
    end
  end

  abstract struct ISlot
    include Indexable::Item(ISlot)
    
    abstract def initialize(@page : IPage, @id : Index, io : IO::Memory)

    def index : Index
      @id
    end

    def indexer : IPage
      @page
    end

    def address : Address
      { indexer.index, index }
    end
  end
  
  struct Slot < ISlot
    SIZE = INT_SIZE*3
    @page = uninitialized Page
    @id = uninitialized Int64

    def initialize(@page : IPage, @id : Index, io : IO::Memory)
      @data = Array(Int64).read_bytes io
    end

    def randomize
      @data = @data.map { |i| rand(Int64) }
    end

    def flush
      @page[@id] = self
    end

    def to_io(io, format)
      io.write_bytes = @data
      @data.each { |i| io.write_bytes i }
    end

    def to_s
      return "#{@data.to_s} #{}"
    end
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

  struct Page < IPage
    INT_SIZE = sizeof(Int64)
    SIZE = 1024
    HEADER_SIZE =  PageHeader::SIZE
    BODY_SIZE =  SIZE - HEADER_SIZE
    SLOT_LINK_SIZE = INT_SIZE*2

    @body : IO::Memory

    def initialize(@manager : IManager, @id : Index, @buffer : IO::Memory)
      @header = PageHeader.new buffer
      @body = buffer.slice
    end

    def from_io(io, format)
    end

    def to_io(io, format)
      io.write_bytes @header
      io.copy_from @body
    end

    def size
      @header.size
    end

    private def slot_link(slot_id)
      io = slot_link_slice slot_id
      {io.read_bytes(Int64), io.read_bytes(Int64)} 
    end

    private def slot_link_slice(slot_id)
      @body[SLOT_LINK_SIZE*slot_id, SLOT_LINK_SIZE]
    end
    
    def unsafe_fetch(slot_id : Int) : ISlot
      offset, content_size = slot_link slot_id
      body_offset = BODY_SIZE - offset - content_size
      Slot.new self, slot_id.to_i64, @body.slice(body_offset, content_size)
    end

    def unsafe_put(slot_id : Int, slot : ISlot)
      offset, content_size = slot_link slot_id
      body_offset = BODY_SIZE - offset - content_size
      @body[body_offset, content_size].write_bytes slot
    end

    def new_slot : ISlot
      # check table has space
      slot_id = @header.size
      @header.size += 1
      offset = slot_id*Slot::SIZE
      size = Slot::SIZE
      {offset, size}.to_io slot_link_slice slot_id
      unsafe_fetch slot_id
    end

    def to_s
      @header.to_s
    end
  end

  struct MemoryManager < IManager
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
    
    def unsafe_put(index : Int, io : IO::Memory)
      unsafe_fetch(index).write io.to_slice
    end
    
    def [](index : Int) : IO::Memory
      puts "copying"
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
