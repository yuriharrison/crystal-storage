require "./spec_helper"

include PageManagement

describe MemoryManager do
  it "#test" do
    page_id = 0.to_i64
    page_manager = MemoryManager.new
    page = Page.new page_manager, page_id, page_manager[page_id]

    slot = page.new_slot
    slot.randomize
    pp! slot.to_s

    slot.flush
    slot_a = page[0]
    page_manager[page_id] = page
    
    page = Page.new page_manager, page_id, page_manager[page_id]
    slot_b = page[0]
    slot_b.address.should eq slot_a.address
    slot_b.to_s.should eq slot_a.to_s
    
  end
end
