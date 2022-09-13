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
        {% unless ann && (ann[:ignore] || ann[:ignore_deserialize]) %}
        @{{ ivar.id }} = io.read_bytes {{ ivar.type }}
        {% end %}
      {% end %}
    {% end %}
  end

  def to_io(io : IO, format=nil)
    {% begin %}
      {% for ivar in @type.instance_vars %}
        {% ann = ivar.annotation(Field) %}
        {% unless ann && (ann[:ignore] || ann[:ignore_deserialize]) %}
          io.write_bytes @{{ ivar.id }}
        {% end %}
      {% end %}
    {% end %}
  end
end
