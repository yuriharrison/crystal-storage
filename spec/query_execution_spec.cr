require "./spec_helper.cr"

include CryStorage::Query
include CryStorage::SQL

describe Query do
  col_id = Column.new("id", DataType::Integer, nil, 1, false, Column::Key::PrimaryKey)
  col_name = Column.new("name", DataType::Text, nil, 2, false, nil)
  col_score = Column.new("score", DataType::BigInt, nil, 3, false, nil)
  col_active = Column.new("active", DataType::Boolean, nil, 4, false, nil)
  columns = Slice[col_id, col_name, col_score, col_active]
  table = Table.new "test_schema", "test_table", columns
  test_values = { 1, "Scott", 100_i64, true }
  slot = PageManagement::Slot.from table, *test_values


  it "test" do
    c1 = Constant.new(1)
    c2 = Constant.new("2")
    filter = And.new c1, c2
    pp! filter.eval
    q = Query.new(
      Slice[col_id, col_name, col_score, col_active],
      nil,
      filter
    )
  end
end
