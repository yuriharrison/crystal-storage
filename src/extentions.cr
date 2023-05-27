struct Bool
  def <(other)
    !self && self != other
  end

  def <=(other)
    !self
  end

  def >(other)
    self && self != other
  end

  def >=(other)
    self
  end
end

# TODO: create a SlotLink serializable class
# then delete the code extension bellow
struct Tuple
  def to_io(io : IO)
    each { |n| io.write_bytes n }
  end
end

class Object
  def tap
    with self yield self
    self
  end
end

class IO::Memory
  def copy(io : IO)
    IO.copy self, io
    io
  end

  def copy_from(io : IO)
    io.copy self
  end
end

class IO::Memory
  def self.build(*args)
    io = Memory.new *args
    with io yield
    io.rewind
  end

  def slice(offet, size) : Memory
    Memory.new to_slice[@pos + offet, size], writeable: @writeable
  end

  def slice
    slice 0, size - @pos
  end

  def [](offset, size)
    slice offset, size
  end

  def copy
    io = IO::Memory.new
    copy io
  end

end

module Indexable::Item(T)
  abstract def index
  abstract def indexer

  def flush
    indexer[index] = self
  end
end
