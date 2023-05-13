require "./spec_helper"

class TestPage < IPage
    getter table

    def initialize(@table : Table)
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

describe Cell do
  it "#from_io #to_io" do
    expected_value = "test string"
    io_value = IO::Memory
      .new
      .tap { write_bytes expected_value }
      .rewind
    IO::Memory.new
      .tap { write_bytes Cell.from_io io_value, String }
      .rewind
      .tap { |io| Cell.from_io(io, String).value.should eq expected_value }
  end

  # it "forwards operations" do
  #   # TODO implement
  #   (Cell.new(10) + Cell.new(10)).should eq 20
  #   (Cell.new(10) ^ Cell.new(10)).should eq 20
  #   (Cell.new("10") + Cell.new("10")).should eq "1010"
  # end
end


describe Table do
  it ".new" do
    col_id = Column.new("id", DataType::Integer, nil, 1_u64, false, Column::Key::PrimaryKey)
    col_name = Column.new("name", DataType::Text, nil, 2_u64, false, nil)
    col_score = Column.new("score", DataType::BigInt, nil, 3_u64, false, nil)
    col_active = Column.new("active", DataType::Boolean, nil, 4_u64, false, nil)
    columns_array = [col_id, col_name, col_score, col_active]
    columns = Slice.new(columns_array.to_unsafe, columns_array.size)
    table = Table.new "test_schema", "test_table", columns

    table.columns.size.should eq 4
    table.bools { |col| col.should eq col_active }
  end
end

describe Slot do
  col_id = Column.new("id", DataType::Integer, nil, 1_u64, false, Column::Key::PrimaryKey)
  col_name = Column.new("name", DataType::Text, nil, 2_u64, false, nil)
  col_score = Column.new("score", DataType::BigInt, nil, 3_u64, false, nil)
  col_active = Column.new("active", DataType::Boolean, nil, 4_u64, false, nil)
  columns_array = [col_id, col_name, col_score, col_active]
  columns = Slice.new(columns_array.to_unsafe, columns_array.size)
  table = Table.new "test_schema", "test_table", columns
  slot = uninitialized Slot

  it ".new" do
    slot = Slot.new TestPage.new(table), 1, IO::Memory.new
  end
end
