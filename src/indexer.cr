
module CryStorage::Indexers
  # TODO Directory class maps table X pages
  # TODO IMPLEMENT SCAN CLASS
  # TODO IMPLEMENT BASIC SELECT WITH SCAN AND INDEX BASED ON WHERE CLAUSE
  # TODO IMPLEMENT JOIN ALGORITHM USING HASH MEMORY
  # TODO IMPLEMENT IMPLEMENT 

  module Indexer
    include Indexable::Mutable(Address)

    abstract def columns
    abstract def range?
  end

  class MemoryHash(K) < Hash(K, Address)
    include Indexer

    def initialize(@column : Column); end

    def columns
      Slice[@column]
    end

    def range?; false; end

    def unsafe_fetch(index)
      self[index]
    end

    def unsafe_put(index, value)
      self[index] = value
    end
  end
end
