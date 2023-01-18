require_relative "test_helper"

class ActiveRecordTest < Minitest::Test
  def setup
    User.delete_all
  end

  def test_relation
    users = 3.times.map { |i| User.create!(name: "User #{i}") }
    df = Polars::DataFrame.new(User.order(:id))
    assert_equal ["id", "name"], df.columns
    assert_series users.map(&:id), df["id"]
    assert_series users.map(&:name), df["name"]
  end

  def test_result
    users = 3.times.map { |i| User.create!(name: "User #{i}") }
    df = Polars::DataFrame.new(User.connection.select_all("SELECT * FROM users ORDER BY id"))
    assert_equal ["id", "name"], df.columns
    assert_series users.map(&:id), df["id"]
    assert_series users.map(&:name), df["name"]
  end

  def test_read_sql_relation
    users = 3.times.map { |i| User.create!(name: "User #{i}") }
    df = Polars.read_sql(User.order(:id))
    assert_equal ["id", "name"], df.columns
    assert_series users.map(&:id), df["id"]
    assert_series users.map(&:name), df["name"]
  end

  def test_read_sql_result
    users = 3.times.map { |i| User.create!(name: "User #{i}") }
    df = Polars.read_sql(User.connection.select_all("SELECT * FROM users ORDER BY id"))
    assert_equal ["id", "name"], df.columns
    assert_series users.map(&:id), df["id"]
    assert_series users.map(&:name), df["name"]
  end
end
