require_relative "test_helper"

class IpcTest < Minitest::Test
  def test_read_ipc
    df = Polars.read_ipc("test/support/data.arrow")
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_read_ipc_file
    df = File.open("test/support/data.arrow", "rb") { |f| Polars.read_ipc(f) }
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_scan_ipc
    df = Polars.scan_ipc("test/support/data.arrow")
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df.collect
  end

  def test_read_ipc_schema
    schema = Polars.read_ipc_schema("test/support/data.arrow")
    assert_equal ({"a" => Polars::Int64, "b" => Polars::Utf8}), schema
  end

  def test_write_ipc
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    path = temp_path
    df.write_ipc(path)
    assert_frame df, Polars.read_ipc(path)
  end

  def test_write_ipc_to_string
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    output = df.write_ipc(nil)
    assert output.start_with?("ARROW")
    assert_equal Encoding::BINARY, output.encoding
  end
end
