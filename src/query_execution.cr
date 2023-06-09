require "./extentions"
require "./indexer"
require "./table"

module CryStorage::SQL
  module ExprValue
    abstract def eval(slot)
  end

  alias Numeric = Int128 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64
  alias ExprValueType = Numeric | Bool | String
  # TODO add all range expr
  alias RangeExpr = LessThan

  abstract class BoolExpr
    include ExprValue
    include Enumerable(BoolExpr)

    @exprs : Array(BoolExpr)?

    abstract def symbol

    def initialize(@left : BoolExpr | ExprValue, @right : BoolExpr | ExprValue)
    end

    def left
      @left
    end

    def right
      @right
    end

    protected def traverse(&block : BoolExpr ->)
      if leaf?
        yield self
        return
      end

      left.unsafe_as(BoolExpr).traverse do |expr|
        block.call expr
      end

      block.call self

      right.unsafe_as(BoolExpr).traverse do |expr|
        block.call expr
      end
    end

    def each
      @exprs ||= begin
        arr = Array(BoolExpr).new
        traverse { |expr| arr << expr }
        arr
      end
      @exprs.not_nil!.each do |expr|
        yield expr
      end
    end

    def leafs
      each { |expr| yield expr if expr.leaf? }
    end

    def leafs
      arr = Array(BoolExpr).new
      leafs { |expr| arr << expr }
      arr
    end

    def leaf?
      !(left.is_a?(BoolExpr) || left.is_a?(BoolExpr))
    end

    def constant : Constant
      case
      when left.is_a? Constant; left.unsafe_as(Constant)
      when right.is_a? Constant; right.unsafe_as(Constant)
      else raise "No constant on expr #{self}"
      end
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

    def eval(slot)
      !!(left.eval(slot) && right.eval(slot))
    end
  end

  class Or < BoolExpr
    
    def symbol; "OR" end

    def eval(slot)
      !!(left.eval(slot) || right.eval(slot))
    end
  end

  class Equal < BoolExpr

    def next
      super
    end

    def symbol; "==" end
    
    def eval(slot)
      left_value = left.eval(slot)
      right_value = right.eval(slot)
      ensureTypes! left_value, right_value
      left_value == right_value
    end
  end

  class LessThan < BoolExpr

    def next
      super
    end

    def symbol; "<" end
    
    def eval(slot)
      left_value = left.eval(slot)
      right_value = right.eval(slot)
      ensureTypes! left_value, right_value
      left_value < right_value
    end
  end

  # class GreatherThan < BoolExpr
  #   def eval(slot)
  #     ensureTypes! ">"
  #     left.eval(slot) > right.eval(slot)
  #   end
  # end

  # class Different < BoolExpr
  #   def eval(slot)
  #     ensureTypes! "!="
  #     left.eval(slot) != right.eval(slot)
  #   end
  # end

  class Attribute
    include ExprValue

    def initialize(@column : Column)
    end

    def ==(other : Attribute)
      @column == other.@column
    end

    def ==(column : Column)
      @column == column
    end

    def eval(slot)
      slot.get @column
    end
  end

  class Constant
    include ExprValue

    def initialize(@value : Int32 | String)
    end

    def eval(slot)
      eval
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
    abstract def schema
    abstract def get(address : Address) : ISlot
    abstract def insert(slot : ISlot)
    abstract def indexer(column : Column, range=false)
  end

  struct PersistentTable
    include Table
    # TODO implement; diff tmp table from actual table
    getter schema : TableSchema
    @indices : Array(Indexer)?
    @pageManager : PageManagement::IManager

    def initialize(@schema, @pageManager)
      initialize(@schema, @pageManager, nil)
    end

    def initialize(@schema, @pageManager, @indices)
    end

    def get(address : Address) : ISlot
      raise "not implemented"
    end

    def insert(slot)
      raise "not implemented"
    end

    def indexer(column : Column, range=false)
      return nil if @indices.nil?

      @indices.not_nil!.find { |indexer|
        indexer.columns.any?(&.==(column)) &&
        (!range || indexer.range?)
      }
    end
  end

  struct JoinTable
    include Table
    getter schema : TableSchema
    @pageManager : PageManagement::IManager

    def initialize(@schema, @pageManager)
      initialize(@schema, @pageManager, nil)
    end

    def get(address : Address) : ISlot
      raise "not implemented"
    end

    def insert(slot)
      raise "not implemented"
    end

    def indexer(column : Column, range=false); end
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

    def initialize(@table, @fields, @joinExprs, @filters)
    end
    
    def each
      # TODO filter fields
      if @joinExprs.nil?
        scan @table do |slot|
          yield slot
        end
      else
        # join @joinExprs do |slot|
        #   yield slot
        # end
      end
    end
    
    # def join(joinExprs)
    #   join_table = nil
    #   joins.each do |expr|
    #     expr.left = join_table unless join_table.nil?
    #     fields = @fields # should be columns of left and right combined
    #     schema = TableSchema.new "tmp_join", "00001", columns
    #     # TODO add expr.right.columns to join_table
    #     join_table = JoinTable.new schema, PageManagement::MemoryManager.default
    #     join expr do |slot|
    #       join_table.insert slot
    #     end
    #   end
    # end

    def join(expr : JoinExpr)
      indexer = MemoryHash(Int32).new expr.condition.leafs.first
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
      # TODO: OPTIMIZE: pre select a exprs to use
      # TODO: OPTIMIZE: allow multi-index match
      expr : SQL::BoolExpr? = nil
      column = table.schema.columns.find do |column|
        expr = @filters.leafs.find do |expr|
          (expr.left.is_a? Attribute && expr.left == column) ||
          (expr.right.is_a? Attribute && expr.right == column)
        end
      end

      # TODO IMPLEVE READABELETY
      range = expr.is_a?(SQL::RangeExpr)
      unless column.nil? || expr.nil?
        indexer = table.indexer column, range
        if indexer && !range
          # TODO group address by page before retrieving
          indexer.scan(expr.constant.eval) do |address|
            yield table.get address
          end
        elsif
          raise "not implemented range scan"
        else
          raise "not implemented full scan"
        end
      else
        raise "not implemented full scan"
      end
    end

  end
end
