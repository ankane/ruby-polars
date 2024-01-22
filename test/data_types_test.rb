require_relative "test_helper"

class DataTypesTest < Minitest::Test
  def test_base_type
    assert_equal Polars::Int64, Polars::Int64.base_type
    assert_equal Polars::List, Polars::List.base_type
    assert_equal Polars::List, Polars::List.new(Polars::Int64).base_type
    assert_equal Polars::Duration, Polars::Duration.new("ns").base_type
  end

  def test_to_s
    assert_equal "Polars::Int64", Polars::Int64.to_s
    assert_equal "Polars::Decimal", Polars::Decimal.to_s
    assert_equal "Polars::Decimal(precision: 15, scale: 1)", Polars::Decimal.new(15, 1).to_s
    assert_equal %!Polars::Datetime(time_unit: "ns", time_zone: nil)!, Polars::Datetime.new("ns").to_s
    assert_equal %!Polars::Duration(time_unit: "ns")!, Polars::Duration.new("ns").to_s
    assert_equal "Polars::List", Polars::List.to_s
    assert_equal "Polars::List(Polars::Int64)", Polars::List.new(Polars::Int64).to_s
    assert_equal "Polars::Array(Polars::Int64)", Polars::Array.new(3, Polars::Int64).to_s
    assert_equal %!Polars::Struct([Polars::Field("a", Polars::Int64)])!, Polars::Struct.new([Polars::Field.new("a", Polars::Int64)]).to_s
  end

  def test_equal_int
    assert_equal Polars::Int64, Polars::Int64
    refute_equal Polars::Int64, Polars::Int32
  end

  def test_equal_float
    assert_equal Polars::Float64, Polars::Float64
    refute_equal Polars::Float64, Polars::Float32
  end

  def test_equal_decimal
    assert_equal Polars::Decimal, Polars::Decimal
    assert_equal Polars::Decimal.new(15, 1), Polars::Decimal.new(15, 1)
    refute_equal Polars::Decimal.new(15, 1), Polars::Decimal.new(25, 1)
    refute_equal Polars::Decimal.new(15, 1), Polars::Decimal.new(15, 2)
    assert_equal Polars::Decimal.new(15, 1), Polars::Decimal
  end

  def test_equal_datetime
    assert_equal Polars::Datetime, Polars::Datetime
    assert_equal Polars::Datetime.new("ns"), Polars::Datetime.new("ns")
    refute_equal Polars::Datetime.new("ns"), Polars::Datetime.new("us")
    assert_equal Polars::Datetime.new("ns"), Polars::Datetime
  end

  def test_equal_duration
    assert_equal Polars::Duration, Polars::Duration
    assert_equal Polars::Duration.new("ns"), Polars::Duration.new("ns")
    refute_equal Polars::Duration.new("ns"), Polars::Duration.new("us")
    assert_equal Polars::Duration.new("ns"), Polars::Duration
  end

  def test_equal_list
    assert_equal Polars::List, Polars::List
    assert_equal Polars::List.new(Polars::Int64), Polars::List.new(Polars::Int64)
    refute_equal Polars::List.new(Polars::Int64), Polars::List.new(Polars::Int32)
    assert_equal Polars::List.new(Polars::Int64), Polars::List
    assert_equal Polars::List, Polars::List.new(Polars::Int64)
  end

  def test_equal_array
    assert_equal Polars::Array, Polars::Array
    assert_equal Polars::Array.new(3, Polars::Int64), Polars::Array.new(3, Polars::Int64)
    refute_equal Polars::Array.new(3, Polars::Int64), Polars::Array.new(3, Polars::Int32)
    assert_equal Polars::Array.new(3, Polars::Int64), Polars::Array
  end

  def test_equal_struct
    assert_equal Polars::Struct, Polars::Struct
    assert_equal Polars::Struct.new([Polars::Field.new("a", Polars::Int64)]), Polars::Struct
    assert_equal Polars::Struct.new([Polars::Field.new("a", Polars::Int64)]), Polars::Struct.new([Polars::Field.new("a", Polars::Int64)])
    refute_equal Polars::Struct.new([Polars::Field.new("a", Polars::Int64)]), Polars::Struct.new([Polars::Field.new("b", Polars::Int64)])
    refute_equal Polars::Struct.new([Polars::Field.new("a", Polars::Int64)]), Polars::Struct.new([Polars::Field.new("a", Polars::Int32)])
  end

  def test_is_numeric
    assert Polars::Int64.numeric?
    refute Polars::String.numeric?
  end

  def test_is_decimal
    assert Polars::Decimal.decimal?
    assert Polars::Decimal.new(15, 1).decimal?
    refute Polars::String.numeric?
  end

  def test_is_integer
    assert Polars::Int64.integer?
    assert Polars::UInt64.integer?
    refute Polars::String.integer?
  end

  def test_is_signed_integer
    assert Polars::Int64.signed_integer?
    refute Polars::UInt64.signed_integer?
    refute Polars::String.signed_integer?
  end

  def test_is_unsigned_integer
    assert Polars::UInt64.unsigned_integer?
    refute Polars::Int64.unsigned_integer?
    refute Polars::String.unsigned_integer?
  end

  def test_is_float
    assert Polars::Float32.float?
    assert Polars::Float64.float?
    refute Polars::Int64.float?
    refute Polars::String.float?
  end

  def test_is_temporal
    assert Polars::Datetime.temporal?
    assert Polars::Datetime.new("ns").temporal?
    assert Polars::Duration.temporal?
    assert Polars::Duration.new("ns").temporal?
    refute Polars::Int64.temporal?
    refute Polars::String.temporal?
  end

  def test_is_nested
    refute Polars::Int64.nested?
    assert Polars::List.nested?
    assert Polars::List.new(Polars::Int64).nested?
  end
end
