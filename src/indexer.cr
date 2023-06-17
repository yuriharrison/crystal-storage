
module CryStorage::Indexers
  # TODO Directory class maps table X pages
  # TODO IMPLEMENT SCAN CLASS
  # TODO IMPLEMENT BASIC SELECT WITH SCAN AND INDEX BASED ON WHERE CLAUSE
  # TODO IMPLEMENT JOIN ALGORITHM USING HASH MEMORY
  # TODO IMPLEMENT IMPLEMENT 

  module Indexer
    abstract def columns : Slice(Column)
    abstract def scan(value : K, & : Address -> _) forall K
    abstract def put(key : K, value : Address) forall K
    abstract def delete(key : K) forall K

    def put(key : K, value : ISlot) forall K
      put key, value.address
    end

    abstract def put(slot : ISlot)

    def range?
      is_a? RangeIndexer
    end
  end

  module RangeIndexer(K)
    include Indexer

    abstract def scan(range : Range)
  end

  class MemoryHash(K) < Hash(K, Address)
    include Indexer

    def initialize(@column : Column)
      initialize
    end

    def put(key, value : Address)
      raise %(
        Key type error: #{key} of type '#{typeof(key)}'.\n
        Expected #{K}
      ) unless key.is_a?(K)

      self[key.unsafe_as(K)] = value
    end

    def put(slot : ISlot) 
      if columns.size == 1
        put slot.get(columns.first), slot
        return
      end

      key = columns.map do |column|
        slot.get column
      end.to_s
      put key, slot
    end

    def columns : Slice(Column)
      Slice[@column]
    end

    def scan(value)
      yield self[value] if self[value]?
    end

  end
end
