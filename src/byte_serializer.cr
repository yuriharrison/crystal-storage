class String
  def self.from_io(io : IO)
    io.read_string io.write_bytes Int32
  end

  def to_io(io : IO)
    io.write_bytes bytesize
    io.write_bytes bytes
    io
  end
end

module Serializer
  annotation Field
  end

  def to_io(io : IO)
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
