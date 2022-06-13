require "./spec_helper"

include PageManagement

describe PageManager do
  it "#test" do
    page_id = 0
    page_manager = PageManager.new
    page = page_manager[page_id]
    puts page.id

    # slot = PageSlot.new page, 1
    # puts slot
    # io = IO::Memory.new
    # slot.to_io io
    # io.rewind
    # slot = PageSlot.new page, 1, io
    # puts slot
  end
end
