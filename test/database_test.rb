require_relative "test_helper"

class DatabaseTest < Minitest::Test
  def setup
    User.delete_all
  end

  def test_relation
    users = create_users
    df = Polars::DataFrame.new(User.order(:id))
    assert_result df, users
  end

  def test_result
    users = create_users
    df = Polars::DataFrame.new(User.connection.select_all("SELECT * FROM users ORDER BY id"))
    assert_result df, users
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
    skip unless ENV["ADAPTER"] == "postgresql"

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
    assert_equal ["id", "name", "number", "inexact", "active", "joined_at"], df.columns
    assert_series users.map(&:id), df["id"]
    assert_series users.map(&:name), df["name"]
    assert_series users.map(&:number), df["number"]
    assert_series users.map(&:inexact), df["inexact"]
    if ENV["ADAPTER"] == "postgresql"
      assert_series users.map(&:active), df["active"]
    end
    assert_series users.map(&:joined_at), df["joined_at"]
    assert_schema df
  end

  def assert_schema(df)
    schema = df.schema
    assert_equal Polars::Int64, schema["id"]
    assert_equal Polars::Utf8, schema["name"]
    assert_equal Polars::Int64, schema["number"]
    assert_equal Polars::Float64, schema["inexact"]
    if ENV["ADAPTER"] == "postgresql"
      assert_equal Polars::Boolean, schema["active"]
      assert_equal Polars::Datetime, schema["joined_at"]
    else
      assert_equal Polars::Int64, schema["active"]
      assert_equal Polars::Utf8, schema["joined_at"]
    end
  end

  def create_users
    3.times.map { |i| User.create!(name: "User #{i}", number: i, inexact: i + 0.5, active: i % 2 == 0, joined_at: Time.now) }
  end
end
