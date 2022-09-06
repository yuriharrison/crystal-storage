class String
  def self.from_io(io : IO)
    io.read_string io.read_bytes Int32
  end

  def to_io(io : IO)
    io.write_bytes bytesize
    bytes.each do |byte|
      io.write_byte byte
    end
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
