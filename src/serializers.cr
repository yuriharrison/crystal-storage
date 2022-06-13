module CryStorage::Serializers
  module IOSerializable
    abstract def initialize(io : IO::Memory)
    abstract def to_io(io : IO::Memory)
    
    {% begin %}
      def self.from_io(io : IO) : {{ @type }}
        new io
      end
    {% end %}
    
    def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::SystemEndian)
      to_io io
    end
  end
end
