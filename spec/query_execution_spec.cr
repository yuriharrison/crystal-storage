require "./spec_helper.cr"

include CryStorage::Query
include CryStorage::SQL

describe Query do
  col_id = Column.new("id", DataType::Integer, nil, 1, false, Column::Key::PrimaryKey)
  col_name = Column.new("name", DataType::Text, nil, 2, false, nil)
  col_score = Column.new("score", DataType::BigInt, nil, 3, false, nil)
  col_active = Column.new("active", DataType::Boolean, nil, 4, false, nil)
  columns = Slice[col_id, col_name, col_score, col_active]
  schema = TableSchema.new "test_schema", "test_table", columns
  pageManager = PageManagement::MemoryManager.default
  index_id = Indexers::MemoryHash(Int32).new col_id
  indexes = [index_id] of Indexer
  table = PersistentTable.new schema, pageManager, indexes

  test_values = { 1, "Scott", 100_i64, true }
  slot = PageManagement::Slot.from schema, *test_values

  it "test" do
    c1 = Constant.new(1)
    c2 = Constant.new("2")
    filter = And.new c1, c2
    pp! filter.eval
    pp! filter.to_s
    filter.each { |expr| puts expr }
    q = Query.new(
      table,
      Slice[col_id, col_name, col_score, col_active],
      nil,
      filter
    )
  end
end
