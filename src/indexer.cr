
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
      self[key.as(K)] = value
    end

    def columns : Slice(Column)
      Slice[@column]
    end

    def scan(value)
      yield self[value] if self[value]?
    end

    def key_type
      K
    end
  end
end
