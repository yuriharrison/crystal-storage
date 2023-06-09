require "./src/*"

arr = [1,1,22,3,4,4,4]
arr2 = [7, 8]

value_b : Int32? = nil
value = arr.find do |n|
  value_b = arr2.find do |m|
    n == m
  end
end


pp! value, value_b
