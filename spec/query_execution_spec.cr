require "./spec_helper.cr"

include CryStorage::Query
include CryStorage::SQL


struct TestTable
  include Table
  
  getter schema : TableSchema
  @store = Hash(Address, ISlot).new
  @indices : Array(Indexer)?
  @page : IPage

  def initialize(@page, @schema, @indices)
  end

  def get(address : Address) : ISlot
    @store[address]
  end

  def insert(slot : ISlot)
    @page.push slot
    
    @indices.not_nil!.each do |indexer|
      indexer.put slot
    end unless @indices.nil?

    @store[slot.address] = slot
  end

  def scan
    @store.each_value do |slot|
      yield slot
    end
  end

  def indexer(column : Column, range=false)
    return nil if @indices.nil?

    @indices.not_nil!.find do |indexer|
      indexer.columns.any?(&.==(column)) &&
      (!range || indexer.range?)
    end
  end
end


describe Query do
  col_id = Column.new("id", DataType::Integer, nil, 1, false, Column::Key::PrimaryKey)
  col_name = Column.new("name", DataType::Text, nil, 2, false, nil)
  col_score = Column.new("score", DataType::BigInt, nil, 3, false, nil)
  col_active = Column.new("active", DataType::Boolean, nil, 4, false, nil)
  columns = Slice[col_id, col_name, col_score, col_active]
  schema = TableSchema.new "test_schema", "test_table", columns
  
  page_manager = PageManagement::MemoryManager.default
  page_id, io = page_manager.new_page
  page = PageManagement::Page(PageManagement::Slot).new page_manager, page_id, schema, io
  
  index_id = Indexers::MemoryHash(Int32).new col_id
  index_name = Indexers::MemoryHash(String).new col_name
  indexes = [index_id, index_name] of Indexer
  table = TestTable.new page, schema, indexes

  test_values = { 1, "Scott", 100_i64, true }
  slot = PageManagement::Slot.from schema, *test_values
  table.insert slot

  it "full scan" do
    q = Query.new(table, Slice[col_id, col_name, col_score, col_active])

    result = q.first
    result.should eq slot
  end

  it "index scan" do
    c1 = Constant.new(1)
    c2 = Attribute.new col_id

    q = Query.new(
      table,
      Slice[col_id, col_name, col_score, col_active],
      filters:And.new c1, c2
    )

    result = q.first
    result.should eq slot
  end

  it "single join" do
    c1 = Attribute.new col_id
    c2 = Attribute.new col_id
    join = JoinExpr.new table, table, And.new(c1, c2)

    q = Query.new(
      table,
      Slice[col_id, col_name, col_score, col_active],
      joinExprs: Slice[join]
      )
    
    q.first.values.zip({ *test_values, *test_values }).each do |value, expected|
      value.should eq expected
    end
  end
  
  it "two joins" do
    c1 = Attribute.new col_id
    c2 = Attribute.new col_id
    join = JoinExpr.new table, table, And.new(c1, c2)

    q = Query.new(
      table,
      Slice[col_id, col_name, col_score, col_active],
      joinExprs: Slice[join, join]
      )
    
    q.first.values.zip({ *test_values, *test_values, *test_values }).each do |value, expected|
      value.should eq expected
    end
  end

end
