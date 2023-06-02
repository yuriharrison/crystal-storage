require "./extentions"
require "./indexer"
require "./table"

module CryStorage::SQL
  module ExprValue
    abstract def eval
  end

  alias Numeric = Int128 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64
  alias ExprValueType = Numeric | Bool | String
  alias ExprFinal = Constant

  abstract class BoolExpr
    include ExprValue
    include Enumerable(BoolExpr)

    abstract def symbol

    def initialize(@left : BoolExpr | ExprFinal, @right : BoolExpr | ExprValue)
    end

    def left
      @left
    end

    def right
      @right
    end

    def each(&block : BoolExpr ->)
      if leaf?
        yield self
        return
      end

      left.unsafe_as(BoolExpr).each do |expr|
        block.call expr
      end

      block.call self

      right.unsafe_as(BoolExpr).each do |expr|
        block.call expr
      end
    end

    def leafs
      each { |expr| yield if expr.leaf? }
    end

    def leaf?
      !(left.is_a?(BoolExpr) || left.is_a?(BoolExpr))
    end

    def to_s(io)
      left.to_s io
      io << " "
      symbol.to_s io
      io << " "
      right.to_s io
    end

    macro ensureTypes!(left_value, right_value)
      raise %(
        Invalid operator #{symbol} for types #{typeof(left_value)} and #{typeof(right_value)}
        Value: #{left_value} < #{right_value}
      ) if !left_value.is_a?(typeof(right_value))
      raise %(
        Invalid operator #{symbol} for types #{typeof(left_value)} and #{typeof(right_value)}
        Value: #{left_value} #{symbol} #{right_value}
      ) if !(left_value.is_a?(Numeric) && right_value.is_a?(Numeric))
    end
  end

  class And < BoolExpr

    def symbol; "AND" end

    def eval
      !!(left.eval && right.eval)
    end
  end

  class Or < BoolExpr
    
    def symbol; "OR" end

    def eval
      !!(left.eval || right.eval)
    end
  end

  class Equal < BoolExpr

    def next
      super
    end

    def symbol; "==" end
    
    def eval
      left_value = left.eval
      right_value = right.eval
      ensureTypes! left_value, right_value
      left_value == right_value
    end
  end

  class LessThan < BoolExpr

    def next
      super
    end

    def symbol; "<" end
    
    def eval()
      left_value = left.eval
      right_value = right.eval
      ensureTypes! left_value, right_value
      left_value < right_value
    end
  end

  # class GreatherThan < BoolExpr
  #   def eval
  #     ensureTypes! ">"
  #     left.eval > right.eval
  #   end
  # end

  # class Different < BoolExpr
  #   def eval
  #     ensureTypes! "!="
  #     left.eval != right.eval
  #   end
  # end

  # class Attribute
  #   include ExprValue

  #   def initialize(@column : String)
  #   end

  #   def eval
  #     slot.get @column
  #   end
  # end

  class Constant
    include ExprValue

    def initialize(@value : ExprValueType)
    end

    def eval
      @value
    end

    def to_s(io)
      @value.to_s io
    end
  end
end

module CryStorage::Query
  include Indexers

  module Table
    abstract def insert(slot)
    abstract def indexer(column, range=false)
  end

  struct PersistentTable
    include Table
    # TODO implement; diff tmp table from actual table
    @schema : TableSchema
    @indices : Array(Indexer)?
    @pageManager : PageManagement::IManager

    def initialize(@schema, @pageManager)
      initialize(@schema, @pageManager, nil)
    end

    def initialize(@schema, @pageManager, @indices)
    end

    def insert(slot)
      # TODO
    end

    def indexer(column, range=false)
      return nil if @indices.nil?

      @indices.find { |indexer|
        (!range || indexer.range?) \
        && indexer.columns.any? &.== column }
    end
  end

  struct JoinTable
     include Table

    def insert(slot)
      # TODO
    end

    def indexer(column, range=false); end
  end

  struct JoinExpr
    property left : Table?
    getter right : Table
    getter condition : SQL::BoolExpr
    getter type = Type::Inner

    def initialize(@right, @condition)
    end

    enum Type
      Inner
      Left
      Right
      Outher
    end
  end

  struct Query
    include Enumerable(PageManagement::ISlot)

    @fields : Slice(Column)
    @joinExprs : Slice(JoinExpr)?
    @filters : SQL::BoolExpr
    @table : Table
    @joinIndexer : Object.class

    def initialize(@table, @fields, @joinExprs, @filters)
      @joinIndexer = MemoryHash
    end
    
    def each
      # TODO filter fields
      if @joinExprs.nil?
        scan @table do |slot|
          yield slot
        end
      else
        join @joinExprs do |slot|
          yield slot
        end
      end
    end
    
    def join(joinExprs)
      join_table = nil
      joins.each do |expr|
        expr.left = join_table unless join_table.nil?
        fields = @fields # should be columns of left and right combined
        schema = TableSchema.new "tmp_join", "00001", columns
        # TODO add expr.right.columns to join_table
        join_table = JoinTable.new schema, PageManagement::MemoryManager.default
        join expr do |slot|
          join_table.insert slot
        end
      end
    end

    def join(expr)
      indexer = @joinIndexer.new
      scan(expr.left) do |slot|
        key = slot.get expr.left_column
        indexer[key] = slot
      end
      scan(expr.right) do |right_slot|
        key = right_slot.get expr.right_column
        left_slot = indexer[key]?
        next unless left_slot
        yield left_slot + right_slot
      end
    end

    def scan(table)
      
    end

  end
end
