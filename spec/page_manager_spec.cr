require "./spec_helper"

include PageManagement

describe MemoryManager do
  it "#test" do
    page_id = 0
    page_manager = MemoryManager.new
    page = page_manager[page_id]
    puts page.index

    slot = page.new_slot
    slot.randomize

    slot.flush
    slot = page[0]
    pp! slot.address
  end
end
