require "./src/*"

alias Numeric = Int128 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64
alias Any = Numeric | String | Bool

a : Any = 10
b : Any = true

# raise "invalid value" if !a.is_a? Numeric || !b.is_a? Numeric

# puts a < b

puts !b.is_a? Numeric
