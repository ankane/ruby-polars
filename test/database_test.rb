require_relative "test_helper"

class DatabaseTest < Minitest::Test
  def setup
    User.delete_all
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
    users = create_users

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
        settings: settings[i]
      )
    end
    # reload for time column
    User.order(:id).to_a
  end

  def postgresql?
    ENV["ADAPTER"] == "postgresql"
  end
end
