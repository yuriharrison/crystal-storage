require "./page_manager"

class Object
  def tabulate
    IO::Memory
      .new
      .tap { |io| self.tabulate io }
      .rewind
      .to_s
  end

  def tabulate(io)
    to_s io
  end
end

module Enumerable
  def tabulate(io)
    io << "| "
    each { |item| io << item.tabulate(io) << " | " }
    io << "\n"
  end
end

class CryStorage::PageManagement::Slot
  def tabulate(io)
    @values.tabulate io
  end
end
