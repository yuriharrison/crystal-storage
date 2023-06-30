require "./extentions"
require "./indexer"
require "./table"

module CryStorage::SQL
  module ExprValue
    abstract def eval(slot)

    def unsafe_as_constant
      unsafe_as Constant
    end

    def unsafe_as_attribute
      unsafe_as Attribute
    end

    def bool_expr?
      is_a? BoolExpr
    end

    def constant?
      is_a? Constant
    end

    def attribute?
      is_a? Attribute
    end
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

    def join_leafs
      each { |expr| yield expr if expr.join_leaf? }
    end

    def join_leafs
      arr = Array(BoolExpr).new
      join_leafs { |expr| arr << expr }
      arr
    end

    def leaf?
      !(left.bool_expr? || right.bool_expr?)
    end

    def join_leaf?
      left.attribute? && right.attribute?
    end

    def table_leaf?
      typeof(left) != typeof(right) && (left.attribute? || right.attribute?)
    end

    def constant : Constant
      case
      when left.constant?
        left.unsafe_as_constant
      when right.constant?
        right.unsafe_as_constant
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

    getter column : Column

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

    def to_s(io)
      @column.to_s(io)
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


  struct JoinExpr
    property left : Table?
    getter right : Table
    getter condition : SQL::BoolExpr
    getter type = Type::Inner

    def initialize(@left, @right, @condition)
    end

    def first_condition
      @condition.join_leafs.first
    end

    def left_column
      first_condition.left.unsafe_as_attribute.column
    end

    def right_column
      first_condition.right.unsafe_as_attribute.column
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
    @filters : SQL::BoolExpr?
    @table : Table

    def initialize(@table, @fields, @joinExprs = nil, @filters = nil)
    end
    
    def each
      # TODO filter fields
      if @joinExprs.nil?
        scan @table do |slot|
          yield slot
        end
      else
        join @joinExprs.not_nil! do |slot|
          yield slot
        end
      end
    end
    
    def join(joinExprs : Slice(JoinExpr))
      join_table = nil
      joinExprs.each do |expr|
        expr.left = join_table unless join_table.nil?
        join_table = RowTable.from expr.left.not_nil!, expr.right.not_nil!
        join expr do |left_slot, right_slot|
          join_table.insert left_slot, right_slot
        end
      end

      join_table.not_nil!.scan do |slot|
        yield slot
      end
    end

    def join(expr : JoinExpr, &)
      # TODO replace ISlot hash table 
      indexer = Hash(Int32, ISlot).new
      scan(expr.left.not_nil!) do |slot|
        key = slot.get(expr.left_column)
        indexer[key] = slot
      end

      scan(expr.right) do |right_slot|
        key = right_slot.get expr.right_column
        left_slot = indexer[key]?
        next unless left_slot
        yield left_slot, right_slot
      end
    end

    def scan(table)
      # TODO: OPTIMIZE: pre select a exprs to use
      # TODO: OPTIMIZE: allow multi-index match
      expr : SQL::BoolExpr? = nil
      column = table.schema.columns.find do |column|
        expr = @filters.not_nil!.leafs.find do |expr|
          (expr.left.is_a? Attribute && expr.left == column) ||
          (expr.right.is_a? Attribute && expr.right == column)
        end
      end unless @filters.nil?

      indexer = table.indexer column, expr.is_a? RangeExpr unless column.nil? || expr.nil?
      
      case indexer
      when .nil?
        table.scan do |slot|
          yield slot
        end
      when .range?
        raise "range scan not implemented"
        # indexer.scan Range.new do |address|
        #   yield table.get address
        # end
      else
        indexer.scan(expr.not_nil!.constant.eval) do |address|
          yield table.get address
        end
      end
    end

  end
end
