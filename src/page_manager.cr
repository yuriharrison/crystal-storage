require "./serializers"

struct Tuple
  def to_io(io : IO)
    each { |n| io.write_bytes n }
  end
end

class IO::Memory::Memory
  def slice(offet, size)
    Memory.new @buffer[@pos + offet, size], writeable: @writeable
  end

  def slice
    slice 0, size - @pos
  end

  def [](offet, size)
    slice offset, size
  end
end

module CryStorage::PageManagement
  alias PageID = Int64
  alias SlotID = Int64
  alias Address = Tuple(PageID, SlotID)
  alias Link = Tuple(Int64, Int64)

  INT_SIZE = sizeof(Int64)

  abstract struct IPageSlot
    include Serializers::IOSerializable

    abstract def initialize(@page : IPage, @id : SlotID, io : IO::Memory)

    def address : Address
      { @page.id, @id }
    end
  end

  abstract class IPage
    include Indexable::Mutable(IPageSlot)
    include Serializers::IOSerializable

    getter id

    abstract def initialize(@manager : IPageManager, @id : PageID, @buffer : IO::Memory)

    abstract def unsafe_fetch(index : SlotID) : IPageSlot

    abstract def unsafe_put(index : SlotID, slot : IPageSlot)
    
    abstract def add(item : IPageSlot) : SlotID

    def <<(item : IPageSlot) : SlotID
      add item
    end

    def flush
      @manager[@id] = self
    end

    def byte_size : Int
      @buffer.size
    end
  end

  abstract class IPageManager
    include Indexable::Mutable(IPage)

    abstract def unsafe_fetch(index : PageID) : IPage

    abstract def unsafe_put(index : PageID, page : IPage)

    abstract def size : Int
  end
  
  struct PageSlot < IPageSlot
    SIZE = INT_SIZE*3
    @page = uninitialized Page
    @id = uninitialized Int64

    def initialize(@page : IPage, @id : SlotID)
      @data = Array(Int64).new 3 { Int64::MIN }
    end

    def initialize(@page : IPage, @id : SlotID, io : IO::Memory)
      initialize io
    end

    def initialize(io : IO::Memory)
      @data = Array(Int64).new 3 { io.read_bytes Int64 }
    end

    def flush
      @page[@id] = self
    end

    def to_io(io : IO::Memory)
      @data.each { |i| io.write_bytes i }
    end
  end
  
  struct PageHeader
    include Serializers::IOSerializable
    SIZE = INT_SIZE*2

    property version
    property size

    def initialize(@version : Int64, @size : Int64)
    end

    def initialize(io : IO::Memory)
      initialize io.read_bytes(Int64), io.read_bytes(Int64)
    end
    
    def to_io(io : IO::Memory)
      io.write_bytes @version
      io.write_bytes @size
    end
  end

  class Page < IPage
    INT_SIZE = sizeof(Int64)
    SIZE = 1024
    HEADER_SIZE =  PageHeader::SIZE
    BODY_SIZE =  SIZE - HEADER_SIZE
    SLOT_LINK_SIZE = INT_SIZE*2
    @header : PageHeader
    @body : Bytes
    @manager = uninitialized IPageManager
    @id = uninitialized PageID
    @header = uninitialized PageHeader
    @body = uninitialized IO::Memory

    def initialize(@manager, @id, buffer)
      initialize buffer
    end

    def initialize(buffer : IO::Memory)
      @header = PageHeader.new buffer
      @body = buffer.slice
    end

    def to_io(io : IO::Memory)
    end

    def size : Int
      @header.size
    end

    private def slot_link(slot_id : Int64)
      io = slot_slice slot_id
      {io.read_bytes Int64, io.read_bytes Int64} 
    end

    private def slot_link_slice(slot_id : Int64)
      @body[SLOT_LINK_SIZE*(slot_id - 1), SLOT_LINK_SIZE]
    end
    
    def unsafe_fetch(slot_id : Int) : IPageSlot
      offset, content_size = slot_link slot_id
      body_offset = BODY_SIZE - offset - content_size
      PageSlot.new self, slot_id, @body.slice(body_offset, content_size)
    end

    def unsafe_put(slot_id : Int, slot : IPageSlot)
      offset, content_size = slot_link slot_id
      body_offset = BODY_SIZE - offset - content_size
      @body[body_offset, content_size].write_bytes slot
    end

    def new_slot : IPageSlot
      # check table has space
      @head.size += 1
      slot_id = @head.size
      offset = slot_id*PageSlot::SIZE
      size = PageSlot::SIZE
      {offset, size}.to_io slot_link_slice slot_id
      unsafe_fetch slot_id
    end

  end

  class PageManager < IPageManager
    @page_size = 4096
    
    def initialize
      # @file = File.open("./db", "r+b")
      # @file.seek()
    end

    def size : Int
      10
    end

    def unsafe_fetch(index : Int) : IPage
      unsafe_fetch index.to_i64
    end

    def unsafe_fetch(index : PageID) : IPage
      Page.new self, index, IO::Memory::Memory.new Slice.new(@page_size, UInt8::MIN)
    end
  
    def unsafe_put(index : Int, page : IPage)

    end
  end
end
