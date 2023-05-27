module Serializable
  annotation Field
  end
  
  macro included
    private def self.new_from_io(io : IO)
      instance = allocate
      instance.initialize(__from_io: io)
      GC.add_finalizer(instance) if instance.responds_to?(:finalize)
      instance
    end

    def self.new(io : IO)
      new_from_io(io)
    end

    def self.from_io(io : IO, format=nil)
      new io
    end

    macro inherited
      def self.new(io : IO)
        new_from_io(io)
      end
    end
  end

  def initialize(*, __from_io io : IO)
    {% begin %}
      {% for ivar in @type.instance_vars %}
        {% ann = ivar.annotation(Field) %}
        {% unless ann && ann[:ignore] %}
        @{{ ivar.id }} = io.read_bytes {{ ivar.type }}
        {% end %}
      {% end %}
    {% end %}
  end

  def to_io(io : IO, format=nil)
    {% begin %}
      {% for ivar in @type.instance_vars %}
        {% ann = ivar.annotation(Field) %}
        {% unless ann && ann[:ignore] %}
          io.write_bytes @{{ ivar.id }}
        {% end %}
      {% end %}
    {% end %}
  end
end

### EXTENTIONS

abstract class IO
  def write_bytes(object : Tuple, format : IO::ByteFormat = IO::ByteFormat::SystemEndian) : IO
    object.to_io(self)
    self
  end

  def write_bytes(object, format : IO::ByteFormat = IO::ByteFormat::SystemEndian) : IO
    object.to_io(self, format)
    self
  end
end

struct Bool
  def self.from_io(io : IO, format=nil)
    io.read_bytes(LibC::Int).unsafe_as(Bool)
  end

  def to_io(io : IO, format=nil)
    io.write_bytes to_unsafe
    io
  end
end

struct Enum
  def self.from_io(io : IO, format=nil)
    from_value io.read_bytes Int32
  end

  def to_io(io : IO, format=nil)
    io.write_bytes value
    io
  end
end

class Array(T)
  def self.from_io(io : IO, format=nil)
    Array.new io.read_bytes(Int32) do
      io.read_bytes T
    end
  end
  
  def to_io(io : IO, format=nil)
    io.write_bytes @size
    each do |item|
      io.write_bytes item
    end
    io
  end

  def byte_size
    size*sizeof(T) + sizeof(Int32)
  end
end

class String
  def self.from_io(io : IO, format=nil)
    io.read_string io.read_bytes Int32
  end

  def to_io(io : IO, format=nil)
    io.write_bytes bytes
    io
  end
end

struct BitArray  
  def self.from_io(io : IO, format=nil)
    size = io.read_bytes(UInt32)
    BitArray
      .new(size)
      .from_slice Bytes.new((size / 8).ceil.to_i) { io.read_bytes UInt8 }
  end

  def from_slice(bytes : Bytes)
    @bits = bytes.to_unsafe.as(Pointer(UInt32))
    self
  end

  def to_io(io : IO, format=nil)
    io.write_bytes size
    # TODO write pointer direclty insated of iterating
    to_slice.each { |value| io.write_bytes value }
    io
  end
end
