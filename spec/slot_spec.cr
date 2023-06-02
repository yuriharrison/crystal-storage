require "./spec_helper"

include CryStorage::PageManagement

class TestPage < IPage
    getter table

    def initialize(@table : TableSchema)
    end

    def size
      1
    end

    def unsafe_fetch(index)
      Slot.new self, 1, IO::Memory.new
    end

    def unsafe_put(index, value)
    end
end

# describe Cell do
#   it "#from_io #to_io" do
#     expected_value = "test string"
#     io_value = IO::Memory
#       .new
#       .tap { write_bytes expected_value }
#       .rewind
#     IO::Memory.new
#       .tap { write_bytes Cell.from_io io_value, String }
#       .rewind
#       .tap { |io| Cell.from_io(io, String).value.should eq expected_value }
#   end

#   it "forwards operations" do
#     (Cell.new("10") + Cell.new("10")).should eq "1010"
#     (Cell.new(10) + Cell.new(10)).should eq 20
#     (Cell.new(10) - Cell.new(10)).should eq 0
#     (Cell.new(10) == Cell.new(10)).should be_truthy
#     (Cell.new(10) != Cell.new(10)).should be_falsey
#     (Cell.new(10) ^ Cell.new(10)).should eq 0
#     (Cell.new(10) & Cell.new(10)).should eq 10
#     (Cell.new(10) | Cell.new(10)).should eq 10
#     (Cell.new(10) << Cell.new(1)).should eq 20
#     (Cell.new(10) >> Cell.new(1)).should eq 5
#   end
# end


describe TableSchema do
  it ".new" do
    col_id = Column.new("id", DataType::Integer, nil, 1, false, Column::Key::PrimaryKey)
    col_name = Column.new("name", DataType::Text, nil, 2, false, nil)
    col_score = Column.new("score", DataType::BigInt, nil, 3, false, nil)
    col_active = Column.new("active", DataType::Boolean, nil, 4, false, nil)
    columns = Slice[col_id, col_name, col_score, col_active]
    table = TableSchema.new "test_schema", "test_table", columns

    table.columns.size.should eq 4
    table.bools { |col| col.should eq col_active }
  end
end

describe Slot do
  col_id = Column.new("id", DataType::Integer, nil, 1, false, Column::Key::PrimaryKey)
  col_name = Column.new("name", DataType::Text, nil, 2, false, nil)
  col_score = Column.new("score", DataType::BigInt, nil, 3, false, nil)
  col_active = Column.new("active", DataType::Boolean, nil, 4, false, nil)
  columns = Slice[col_id, col_name, col_score, col_active]
  table = TableSchema.new "test_schema", "test_table", columns
  slot = uninitialized Slot

  test_values = { 1, "Scott", 100_i64, true }

  check_test_values = -> (slot : Slot) {
    columns.each_with_index do |col, i|
      slot.get(col.name).should eq test_values[i]
    end
  }

  it ".from" do
    slot = Slot.from table, *test_values
  end

  it "#get" do
    check_test_values.call slot
  end

  it "#to_io" do
    slot = Slot.new(table, IO::Memory.build { write_bytes slot })
    check_test_values.call slot
  end

  it "#set" do
    previous_value = slot.get "score"
    slot.set "score", previous_value.as(Int64) + 100_i64
    slot.get("score").should_not eq previous_value
    
    # rollback changes
    slot.set "score", previous_value
    check_test_values.call slot
  end
end
