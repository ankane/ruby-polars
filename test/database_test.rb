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

  def test_read_database_null
    skip unless postgresql?

    User.create!
    df = Polars.read_database("SELECT * FROM users ORDER BY id")
    assert_schema df
  end

  def test_read_database_unsupported
    error = assert_raises(ArgumentError) do
      Polars.read_database(Object.new)
    end
    assert_equal "Expected ActiveRecord::Relation, ActiveRecord::Result, or String", error.message
  end

  private

  def assert_result(df, users)
    assert_equal ["id", "name", "number", "inexact", "active", "joined_at", "joined_on", "bin", "dec", "txt", "joined_time"], df.columns
    assert_series users.map(&:id), df["id"]
    assert_series users.map(&:name), df["name"]
    assert_series users.map(&:number), df["number"]
    assert_series users.map(&:inexact), df["inexact"]

    if postgresql?
      assert_series users.map(&:active), df["active"]
    else
      assert_series users.map(&:active).map { |v| v ? 1 : 0 }, df["active"]
    end

    assert_series users.map(&:joined_at), df["joined_at"]

    if postgresql?
      assert_series users.map(&:joined_on), df["joined_on"]
    else
      assert_series users.map(&:joined_on).map(&:to_s), df["joined_on"]
    end

    assert_series users.map(&:bin), df["bin"]
    assert_series users.map(&:dec), df["dec"]
    assert_series users.map(&:txt), df["txt"]
    assert_series users.map(&:joined_time), df["joined_time"]

    assert_schema df
  end

  def assert_schema(df)
    schema = df.schema
    assert_equal Polars::Int64, schema["id"]
    assert_equal Polars::Utf8, schema["name"]
    assert_equal Polars::Int64, schema["number"]
    assert_equal Polars::Float64, schema["inexact"]
    if postgresql?
      assert_equal Polars::Boolean, schema["active"]
      assert_equal Polars::Datetime, schema["joined_at"]
      assert_equal Polars::Binary, schema["bin"]
      assert_equal Polars::Decimal, schema["dec"]
      assert_equal Polars::Utf8, schema["txt"]
      assert_equal Polars::Time, schema["joined_time"]
    else
      assert_equal Polars::Int64, schema["active"]
      assert_equal Polars::Utf8, schema["joined_at"]
      assert_equal Polars::Binary, schema["bin"]
      assert_equal Polars::Float64, schema["dec"]
      assert_equal Polars::Utf8, schema["txt"]
      assert_equal Polars::Utf8, schema["joined_time"]
    end
  end

  def create_users
    # round time since Postgres only stores microseconds
    now = postgresql? ? Time.now.round(6) : Time.now
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
        joined_time: now
      )
    end
    # reload for time column
    User.order(:id).to_a
  end

  def postgresql?
    ENV["ADAPTER"] == "postgresql"
  end
end
