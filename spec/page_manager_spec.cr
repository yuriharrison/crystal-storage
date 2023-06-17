require "./spec_helper"

include PageManagement

class TestSlot < ISlot
  MIN_CAPACITY = 3
  MAX_CAPACITY = 10
  # TODO: add dinamic compute
  MIN_SIZE = INT_SIZE*MIN_CAPACITY + sizeof(Int32)*2
  @data : Array(Int64)
  @capacity = MIN_CAPACITY
  property status : SlotStatus = SlotStatus.from_value(0)
  property page : Page(TestSlot)? = nil
  not_nil page
  @id : Int32? = nil
  not_nil id
  
  def initialize
    @data = Array(Int64).new @capacity { 0_i64 }
  end

  def initialize(@page : IPage, @id : Index, io : IO::Memory)
    @status = SlotStatus.from_io io
    @data = Array(Int64).from_io io
  end

  def indexer : IPage
    page!
  end
  
  def id=(value : Index)
    @id = value
  end
  
  def id : Index
    id!
  end

  def randomize
    @data.fill { rand(Int64) }
  end

  def randomize_size
    @capacity = begin
      value = @capacity
      until value != @capacity; value = rand(MAX_CAPACITY) end
      value
    end
    
    initialize
    randomize
  end

  def delete
    @status |= SlotStatus::Deleted
  end

  def deleted?
    @status.deleted?
  end

  def byte_size
    sizeof(SlotStatus) + @data.byte_size
  end

  def flush
    page![id!] = self
  end

  def to_io(io, format)
    io.write_bytes @status
    io.write_bytes @data
  end

  def to_s
    return "TestSlot(Page: #{page!.size}, data: #{@data})"
  end
end

describe Page(TestSlot) do
  page_id = 0
  page_manager = MemoryManager.new
  # table = TableSchema.new(
  #   "test_schema",
  #   "test_table",
  #   Slice.new 1 { Column.new("column", DataType::Boolean, nil, 1, false, nil).as(Column) },
  # )
  table = uninitialized TableSchema
  page = Page(TestSlot).new page_manager, page_id, table, page_manager[page_id]
  slot_a = TestSlot.new.tap { randomize }
  slot_b = TestSlot.new.tap { randomize }

  it "#push" do
    page.push slot_a
    slot_a.flush
    
    page.push slot_b
    slot_b.flush

    slot_a.address.should_not eq slot_b.address
    slot_a.address.to_s.should_not eq slot_b.address.to_s

    page.size.should eq 2
  end

  it "[]" do
    page_manager[page_id] = page

    page = Page(TestSlot).new page_manager, page_id, table, page_manager[page_id]
    slot_a_refetch = page[slot_a.id]
    slot_a_refetch.address.should eq slot_a.address
    slot_a_refetch.to_s.should eq slot_a.to_s
  end

  it "[]=" do
    old_state = slot_a.to_s
    expected_address = slot_a.address
    page[slot_a.id] = slot_a.tap { randomize }
    slot_a = page[slot_a.id]

    slot_a.to_s.should_not eq old_state
    slot_a.address.should eq expected_address
  end

  it "variable size slot update" do
    old_state = slot_a.to_s
    old_byte_size = slot_a.byte_size
    expected_address = slot_a.address

    page[slot_a.id] = slot_a.tap { randomize_size }
    slot_a = page[slot_a.id]

    slot_a.to_s.should_not eq old_state
    slot_a.byte_size.should_not eq old_byte_size
    slot_a.address.should eq expected_address
  end


  it "#each" do
    initial_size = page.size
    TestSlot
      .new
      .tap { randomize }
      .tap { |slot| page.push slot }
      .tap { delete }
      .tap { flush }
    
    page.size.should eq initial_size + 1
    count = 0
    page.each do |slot|
      count += 1
    end
    count.should eq initial_size
  end

  it "#full?" do
    page_id = 0
    page_manager = MemoryManager.new
    page = Page(TestSlot).new page_manager, page_id, table, page_manager[page_id]

    until page.full? TestSlot::MIN_SIZE
      page.push TestSlot.new.tap { randomize }
    end

    page.size.should eq (Page::BODY_SIZE/page.slot_cost(TestSlot::MIN_SIZE)).to_i
  end

  describe "TestSlot#delete" do
    it "#delete" do
      TestSlot.new
        .tap { deleted?.should be_falsey }
        .tap { randomize }
        .tap { delete }
        .tap { deleted?.should be_truthy }
    end
  end
end
