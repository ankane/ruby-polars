require_relative "test_helper"

class StructExprTest < Minitest::Test
  def test_get
    assert_expr struct_expr[0]
    assert_expr struct_expr["a"]

    error = assert_raises(ArgumentError) do
      struct_expr[Object.new]
    end
    assert_equal "expected type Integer or String, got Object", error.message
  end

  def test_field
    assert_expr struct_expr.field("a")
  end

  def test_rename_fields
    assert_expr struct_expr.rename_fields(["a"])
  end

  def struct_expr
    Polars.col("a").struct
  end
end
