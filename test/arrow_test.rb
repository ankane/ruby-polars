require_relative "test_helper"

class ArrowTest < Minitest::Test
  def test_series_from_arrow
    require "nanoarrow"

    arr = Nanoarrow::Array.new([1, nil, 3], Nanoarrow.int64)
    s = Polars::Series.new(arr)
    assert_series [1, nil, 3], s, dtype: Polars::Int64
  end

  def test_series_to_arrow
    s = Polars::Series.new("a", [1, nil, 3])
    arr = s.to_arrow
    assert_kind_of Nanoarrow::Array, arr
    assert_equal "int64", arr.schema.type
    assert_equal [1, nil, 3], arr.to_a
  end

  def test_data_frame_from_arrow
    require "nanoarrow"

    rows = [{"a" => 1, "b" => "one"}, {"a" => 2, "b" => nil}, {"a" => nil, "b" => "three"}]
    arr = Nanoarrow::Array.new(rows, Nanoarrow.struct({"a" => Nanoarrow.int64, "b" => Nanoarrow.string}))
    assert_equal rows, Polars::DataFrame.new(arr).to_a
  end

  def test_data_frame_to_arrow
    df = Polars::DataFrame.new({"a" => [1, 2, nil], "b" => ["one", nil, "three"]})
    arr = df.to_arrow
    assert_kind_of Nanoarrow::Array, arr
    assert_equal [1, 2, nil], arr.child(0).to_a
    assert_equal ["one", nil, "three"], arr.child(1).to_a
  end

  def test_schema_from_arrow
    require "nanoarrow"

    schema = Nanoarrow.struct({"a" => Nanoarrow.int64, "b" => Nanoarrow.string})
    assert_equal ({"a" => Polars::Int64, "b" => Polars::String}), Polars::Schema.new(schema).to_h

    error = assert_raises(ArgumentError) do
      Polars::Schema.new(Nanoarrow.int64)
    end
    assert_match "arrow_c_schema of object passed to Polars::Schema did not return struct dtype", error.message
  end

  def test_schema_to_arrow
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    schema = df.schema.to_arrow
    assert_kind_of Nanoarrow::Schema, schema
    assert_equal ["int64", "string_view"], schema.fields.map(&:type)
  end

  def test_arrow_c_stream
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})

    capsule = df.arrow_c_stream
    assert_kind_of Polars::Capsule, capsule
    assert_equal "arrow_array_stream", capsule.name
    assert_kind_of Integer, capsule.to_i

    stream = Struct.new(:arrow_c_stream).new(capsule)
    assert_frame df, Polars::DataFrame.new(stream)

    error = assert_raises(ArgumentError) do
      Polars::DataFrame.new(stream)
    end
    assert_equal "the C stream was already released", error.message
  end

  def test_arrow_c_schema
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    schema = df.schema

    capsule = schema.arrow_c_schema
    assert_kind_of Polars::ArrowSchema, capsule
    assert_kind_of Integer, capsule.to_i

    arrow_schema = Struct.new(:arrow_c_schema).new(capsule)
    assert_equal schema.to_h, Polars::Schema.new(arrow_schema).to_h
  end
end
