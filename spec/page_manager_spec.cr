require "./spec_helper"

include PageManagement

describe MemoryManager do
  it "#test" do
    page_id = 0.to_i64
    page_manager = MemoryManager.new
    page = Page.new page_manager, page_id, page_manager[page_id]
    puts page.index

    slot = page.new_slot
    slot.randomize

    slot.flush
    slot = page[0]
    pp! slot.address
    pp! slot.to_s
    puts page.to_s
    page_manager[page_id] = page

    page = Page.new page_manager, page_id, page_manager[page_id]
    puts page.to_s
    slot = page[0]
    pp! slot.address
    pp! slot.to_s

  end
end
