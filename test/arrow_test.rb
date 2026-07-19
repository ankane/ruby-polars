require_relative "test_helper"

class ArrowTest < Minitest::Test
  def test_series_from_arrow
    require "nanoarrow"

    arr = Nanoarrow::Array.new([1, 2, 3], Nanoarrow.int64)
    s = Polars::Series.new(arr)
    assert_series [1, 2, 3], s, dtype: Polars::Int64
  end

  def test_series_to_arrow
    s = Polars::Series.new("a", [1, 2, 3])
    arr = s.to_arrow
    assert_kind_of Nanoarrow::Array, arr
    assert_equal "int64", arr.schema.type
    assert_equal [1, 2, 3], arr.to_a
  end

  def test_data_frame_to_arrow
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    arr = df.to_arrow
    assert_kind_of Nanoarrow::Array, arr
    assert_equal [1, 2, 3], arr.child(0).to_a
    assert_equal ["one", "two", "three"], arr.child(1).to_a
  end

  def test_schema_to_arrow
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    schema = df.schema.to_arrow
    assert_kind_of Nanoarrow::Schema, schema
    assert_equal ["int64", "string_view"], schema.fields.map(&:type)
  end

  def test_arrow_c_stream
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})

    stream = df.arrow_c_stream
    assert_kind_of Polars::Capsule, stream
    assert_kind_of Integer, stream.to_i

    stream_object = Struct.new(:arrow_c_stream).new(stream)
    assert_frame df, Polars::DataFrame.new(stream_object)

    error = assert_raises(ArgumentError) do
      Polars::DataFrame.new(stream_object)
    end
    assert_equal "the C stream was already released", error.message
  end

  def test_arrow_c_schema
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    schema = df.schema

    arrow_schema = schema.arrow_c_schema
    assert_kind_of Polars::ArrowSchema, arrow_schema
    assert_kind_of Integer, arrow_schema.to_i

    schema_object = Struct.new(:arrow_c_schema).new(arrow_schema)
    assert_equal schema.to_h, Polars::Schema.new(schema_object).to_h
  end
end
