require "./extentions"

module CryStorage::PageManagement
  @[Flags]
  enum SlotStatus
    Locked
    Deleted
  end

  abstract class ISlot
    # TODO: convert interfaces in modules
    include Indexable::Item(ISlot)

    abstract def byte_size
    abstract def delete
    abstract def deleted?

    def address : Address
      { indexer.index, index }
    end
  end
end



# TODO: index slot
# TODO: Write table slot, must support column types Integer Varchar and Boolean
