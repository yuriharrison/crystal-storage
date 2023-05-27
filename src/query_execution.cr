require "./extentions"
require "./indexer"
require "./table"

module CryStorage::SQL
  module ExprValue
    abstract def eval
  end

  alias Numeric = Int128 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64
  alias ExprValueType = Numeric | Bool | String

  abstract class BoolExpr
    include ExprValue

    def initialize(@left : BoolExpr | ExprValue, @right : BoolExpr | ExprValue)
    end

    def left
      @left
    end

    def right
      @right
    end

    macro ensureTypes!(operation, left_value, right_value)
      raise %(
        Invalid operator < for types #{typeof(left_value)} and #{typeof(right_value)}
        Value: #{left_value} < #{right_value}
      ) if !left_value.is_a?(typeof(right_value))
      raise %(
        Invalid operator < for types #{typeof(left_value)} and #{typeof(right_value)}
        Value: #{left_value} < #{right_value}
      ) if !(left_value.is_a?(Numeric) && right_value.is_a?(Numeric))
    end
  end

  class And < BoolExpr
    def eval
      !!(left.eval && right.eval)
    end
  end

  class Or < BoolExpr
    def eval
      !!(left.eval || right.eval)
    end
  end

  class Equal < BoolExpr
    def eval
      left_value = left.eval
      right_value = right.eval
      ensureTypes! "==", left_value, right_value
      left_value == right_value
    end
  end

  class LessThan < BoolExpr
    def eval()
      left_value = left.eval
      right_value = right.eval
      ensureTypes! "<", left_value, right_value
      # raise "test" if left_value.is_a?(Bool) || right_value.is_a?(Bool)
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
  end
end

module CryStorage::Query
  def execution (query)

  end

  class Join
    @left : Table
    @right : Table
    @type = Type::Inner

    def initialize(@left, @right)
    end

    enum Type
      Inner
      Left
      Right
      Outher
    end
  end

  class Query
    @fields : Slice(Column)
    @join : Slice(Join)?
    @filters : SQL::BoolExpr

    def initialize(@fields, @join, @filters)
    end
    
  end
end
