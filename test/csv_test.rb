require_relative "test_helper"

class CsvTest < Minitest::Test
  def test_read_csv
    df = Polars.read_csv("test/support/data.csv")
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_read_csv_file
    df = File.open("test/support/data.csv", "rb") { |f| Polars.read_csv(f) }
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_read_csv_pathname
    require "pathname"

    df = Polars.read_csv(Pathname.new("test/support/data.csv"))
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_read_csv_io
    io = StringIO.new(File.binread("test/support/data.csv"))
    df = Polars.read_csv(io)
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_read_csv_uri
    require "uri"

    # skip remote call
    # Polars.read_csv(URI("https://..."))
  end

  def test_read_csv_http
    error = assert_raises(ArgumentError) do
      Polars.read_csv("http://www.example.com")
    end
    assert_equal "use URI(...) for remote files", error.message
  end

  def test_read_csv_https
    error = assert_raises(ArgumentError) do
      Polars.read_csv("https://www.example.com")
    end
    assert_equal "use URI(...) for remote files", error.message
  end

  def test_read_csv_glob
    expected = {
      a: [1, 2, 3, 4, 5],
      b: ["one", "two", "three", "four", "five"]
    }
    assert_frame expected, Polars.read_csv("test/support/data*.csv")
  end

  def test_read_csv_glob_mismatch
    error = assert_raises(Polars::Error) do
      Polars.read_csv("test/support/*.csv")
    end
    assert_match "schema lengths differ", error.message
  end

  def test_read_csv_batched
    reader = Polars.read_csv_batched("test/support/data.csv")
    batch = reader.next_batches(5)
    assert_equal 1, batch.size
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, batch[0]
    assert_nil reader.next_batches(5)
  end

  def test_scan_csv
    df = Polars.scan_csv("test/support/data.csv")
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df.collect
  end

  def test_write_csv
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    path = temp_path
    assert_nil df.write_csv(path)
    assert_equal "a,b\n1,one\n2,two\n3,three\n", File.read(path)
  end

  def test_write_csv_to_string
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_equal "a,b\n1,one\n2,two\n3,three\n", df.write_csv
  end

  def test_to_csv
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_equal "a,b\n1,one\n2,two\n3,three\n", df.to_csv
  end

  def test_sink_csv
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    path = temp_path
    assert_nil df.lazy.sink_csv(path)
    assert_equal "a,b\n1,one\n2,two\n3,three\n", File.read(path)
    assert_frame df, Polars.read_csv(path)
  end

  def test_has_header_true
    df = Polars.read_csv("test/support/data.csv", has_header: true)
    assert_equal ["a", "b"], df.columns
    assert_equal 3, df.height
  end

  def test_has_header_false
    df = Polars.read_csv("test/support/data.csv", has_header: false)
    assert_equal ["column_1", "column_2"], df.columns
    assert_equal 4, df.height
  end
end
