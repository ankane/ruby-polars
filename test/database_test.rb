require_relative "test_helper"

class DatabaseTest < Minitest::Test
  def setup
    User.delete_all
    ActiveRecord::Base.connection_pool.with_connection do |connection|
      connection.drop_table("items") if connection.table_exists?("items")
    end
  end

  def test_relation
    error = assert_raises(ArgumentError) do
      Polars::DataFrame.new(User.order(:id))
    end
    assert_equal "Use read_database instead", error.message
  end

  def test_result
    error = assert_raises(ArgumentError) do
      Polars::DataFrame.new(User.connection.select_all("SELECT * FROM users ORDER BY id"))
    end
    assert_equal "Use read_database instead", error.message
  end

  def test_read_database_relation
    users = create_users
    df = Polars.read_database(User.order(:id))
    assert_result df, users
  end

  def test_read_database_result
    users = create_users
    df = Polars.read_database(User.connection.select_all("SELECT * FROM users ORDER BY id"))
    assert_result df, users
  end

  def test_read_database_string
    users = create_users
    df = Polars.read_database("SELECT * FROM users ORDER BY id")
    assert_result df, users
  end

  def test_read_database_schema_overrides
    create_users

    df = Polars.read_database("SELECT id FROM users ORDER BY id")
    assert_equal Polars::Int64, df["id"].dtype

    df = Polars.read_database("SELECT id FROM users ORDER BY id", schema_overrides: {"id" => Polars::Int16})
    assert_equal Polars::Int16, df["id"].dtype

    df = Polars.read_database("SELECT id FROM users ORDER BY id", schema_overrides: {id: Polars::Int16})
    assert_equal Polars::Int16, df["id"].dtype
  end

  def test_read_database_null
    User.create!
    df = Polars.read_database("SELECT * FROM users ORDER BY id")
    if postgresql?
      assert_schema df
    else
      df.dtypes[1..].each do |dtype|
        assert_equal Polars::Null, dtype
      end
    end
  end

  def test_read_database_unsupported
    error = assert_raises(ArgumentError) do
      Polars.read_database(Object.new)
    end
    assert_equal "Expected ActiveRecord::Relation, ActiveRecord::Result, or String", error.message
  end

  def test_connection_leasing
    ActiveRecord::Base.connection_handler.clear_active_connections!
    assert_nil ActiveRecord::Base.connection_pool.active_connection?
    ActiveRecord::Base.connection_pool.with_connection do
      Polars.read_database(User.order(:id))
      Polars.read_database("SELECT * FROM users ORDER BY id")
    end
    assert_nil ActiveRecord::Base.connection_pool.active_connection?
  end

  def test_write_database
    df = Polars::DataFrame.new({"a" => ["one", "two", "three"], "b" => [1, 2, 3]})
    assert_equal 3, df.write_database("items")
    assert_frame df, Polars.read_database("SELECT * FROM items")

    error = assert_raises(ArgumentError) do
      df.write_database("items")
    end
    assert_equal "Table already exists", error.message
  end

  def test_write_database_connection
    df = Polars::DataFrame.new({"a" => ["one", "two", "three"], "b" => [1, 2, 3]})
    ActiveRecord::Base.connection_pool.with_connection do |connection|
      assert_equal 3, df.write_database("items", connection)
    end
    assert_frame df, Polars.read_database("SELECT * FROM items")
  end

  def test_if_table_exists_fail
    df = Polars::DataFrame.new({"a" => ["one", "two", "three"], "b" => [1, 2, 3]})
    df.write_database("items", if_table_exists: "fail")

    error = assert_raises(ArgumentError) do
      df.write_database("items", if_table_exists: "fail")
    end
    assert_equal "Table already exists", error.message
  end

  def test_if_table_exists_append
    df = Polars::DataFrame.new({"a" => ["one", "two", "three"], "b" => [1, 2, 3]})
    df.write_database("items", if_table_exists: "append")

    df2 = Polars::DataFrame.new({"a" => ["four", "five"], "b" => [4, 5]})
    df2.write_database("items", if_table_exists: "append")

    assert_frame df.vstack(df2), Polars.read_database("SELECT * FROM items")
  end

  def test_if_table_exists_replace
    df = Polars::DataFrame.new({"a" => ["one", "two", "three"], "b" => [1, 2, 3]})
    df.write_database("items", if_table_exists: "replace")

    df2 = Polars::DataFrame.new({"a" => ["four", "five"], "b" => [4, 5]})
    df2.write_database("items", if_table_exists: "replace")

    assert_frame df2, Polars.read_database("SELECT * FROM items")
  end

  def test_if_table_exists_invalid
    df = Polars::DataFrame.new({"a" => ["one", "two", "three"], "b" => [1, 2, 3]})
    error = assert_raises(ArgumentError) do
      df.write_database("items", if_table_exists: "invalid")
    end
    assert_equal %!write_database `if_table_exists` must be one of ["append", "replace", "fail"], got "invalid"!, error.message
  end

  def test_write_database_types
    time = Time.now
    df =
      Polars::DataFrame.new([
        Polars::Series.new("binary", ["bin".b], dtype: Polars::Binary),
        Polars::Series.new("boolean", [true], dtype: Polars::Boolean),
        Polars::Series.new("date", [Date.today], dtype: Polars::Date),
        Polars::Series.new("datetime", [time], dtype: Polars::Datetime),
        Polars::Series.new("decimal", [BigDecimal("123456789.01234567890123456789")], dtype: Polars::Decimal),
        Polars::Series.new("float32", [1.5], dtype: Polars::Float32),
        Polars::Series.new("float64", [Float::MAX], dtype: Polars::Float64),
        Polars::Series.new("int8", [(1 << 7) - 1], dtype: Polars::Int8),
        Polars::Series.new("int16", [(1 << 15) - 1], dtype: Polars::Int16),
        Polars::Series.new("int32", [(1 << 31) - 1], dtype: Polars::Int32),
        Polars::Series.new("int64", [(1 << 63) - 1], dtype: Polars::Int64),
        Polars::Series.new("uint8", [(1 << 8) - 1], dtype: Polars::UInt8),
        Polars::Series.new("uint16", [(1 << 16) - 1], dtype: Polars::UInt16),
        Polars::Series.new("uint32", [(1 << 32) - 1], dtype: Polars::UInt32),
        # Polars::Series.new("uint64", [(1 << 64) - 1], dtype: Polars::UInt64),
        Polars::Series.new("string", ["str"], dtype: Polars::String),
        Polars::Series.new("time", [time], dtype: Polars::Time)
      ])
    df.write_database("items")

    result = Polars.read_database("SELECT * FROM items")
    if postgresql? || mysql?
      assert_equal time, result["datetime"][0]
      assert_equal "123456789.01234567890123456789", result["decimal"][0].to_s
    else
      assert_equal time, result["datetime"][0]
      # TODO fix or raise error
      assert_equal "123456789.01234567", result["decimal"][0].to_s
    end
  end

  def test_write_database_many_rows
    df = Polars::DataFrame.new({"a" => 1..100_000})
    df.write_database("items")
  end

  private

  def assert_result(df, users)
    assert_series users.map(&:id), df["id"]
    assert_series users.map(&:name), df["name"]
    assert_series users.map(&:number), df["number"]
    assert_series users.map(&:inexact), df["inexact"]
    assert_series users.map(&:joined_at), df["joined_at"]
    assert_series users.map(&:bin), df["bin"]
    assert_series users.map(&:dec), df["dec"]
    assert_series users.map(&:txt), df["txt"]
    assert_series users.map(&:joined_time), df["joined_time"]

    if postgresql?
      assert_series users.map(&:active), df["active"]
      assert_series users.map(&:joined_on), df["joined_on"]
    elsif mysql?
      assert_series users.map(&:active).map { |v| v ? 1 : 0 }, df["active"]
      assert_series users.map(&:joined_on), df["joined_on"]
    else
      assert_series users.map(&:active).map { |v| v ? 1 : 0 }, df["active"]
      assert_series users.map(&:joined_on).map(&:to_s), df["joined_on"]
    end

    assert_schema df
  end

  def assert_schema(df)
    schema = df.schema

    assert_equal Polars::Int64, schema["id"]
    assert_equal Polars::String, schema["name"]
    assert_equal Polars::Int64, schema["number"]
    assert_equal Polars::Float64, schema["inexact"]
    assert_equal Polars::Binary, schema["bin"]
    assert_equal Polars::String, schema["txt"]

    if postgresql?
      assert_equal Polars::Boolean, schema["active"]
      assert_equal Polars::Datetime, schema["joined_at"]
      assert_equal Polars::Decimal, schema["dec"]
      assert_equal Polars::Time, schema["joined_time"]
      # TODO fix for null
      # assert_equal Polars::Struct, schema["settings"]
    elsif mysql?
      assert_equal Polars::Int64, schema["active"]
      assert_equal Polars::Datetime, schema["joined_at"]
      assert_equal Polars::Decimal, schema["dec"]
      assert_equal Polars::Datetime, schema["joined_time"]
      # assert_equal Polars::String, schema["settings"]
    else
      assert_equal Polars::Int64, schema["active"]
      assert_equal Polars::String, schema["joined_at"]
      assert_equal Polars::Float64, schema["dec"]
      assert_equal Polars::String, schema["joined_time"]
      assert_equal Polars::String, schema["settings"]
    end
  end

  def create_users
    # round time since Postgres only stores microseconds
    now = postgresql? ? Time.now.round(6) : Time.now
    # TODO fix nil
    settings = [{"hello" => "world"}, {}, {}]
    3.times do |i|
      User.create!(
        name: "User #{i}",
        number: i,
        inexact: i + 0.5,
        active: i % 2 == 0,
        joined_at: now + i,
        joined_on: Date.today + i,
        bin: "bin".b,
        dec: BigDecimal("1.5"),
        txt: "txt",
        joined_time: now,
        settings: mysql? ? settings[i].to_json : settings[i]
      )
    end
    # reload for time column
    User.order(:id).to_a
  end

  def postgresql?
    ENV["ADAPTER"] == "postgresql"
  end

  def mysql?
    ["mysql", "trilogy"].include?(ENV["ADAPTER"])
  end
end
