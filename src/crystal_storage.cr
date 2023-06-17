module CryStorage
  alias Index = Int32
  alias Address = Tuple(Index, Index)
end

require "./utils.cr"
require "./extentions.cr"
require "./byte_serializer.cr"
require "./page_manager.cr"
require "./slot.cr"
require "./indexer.cr"
require "./table.cr"
require "./query_execution.cr"
