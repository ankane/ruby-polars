require_relative "test_helper"

class PlotTest < Minitest::Test
  def test_default_type_column
    df = Polars::DataFrame.new({"a" => ["one", "two", "three"], "b" => [1, 2, 3]})
    assert_plot_type "column", df.plot("a", "b")
  end

  def test_default_type_scatter
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [4, 5, 6]})
    assert_plot_type "scatter", df.plot("a", "b")
  end

  def test_default_type_line
    today = Date.today
    df = Polars::DataFrame.new({"a" => [today - 2, today - 1, today], "b" => [1, 2, 3]})
    assert_plot_type "line", df.plot("a", "b")
  end

  def test_default_columns_not_two
    df = Polars::DataFrame.new({"a" => ["one", "two", "three"]})
    error = assert_raises(ArgumentError) do
      df.plot("a")
    end
    assert_equal "Must specify columns", error.message
  end

  def test_type
    df = Polars::DataFrame.new({"a" => ["one", "two", "three"], "b" => [1, 2, 3]})
    assert_plot_type "pie", df.plot("a", "b", type: "pie")
    assert_plot_type "line", df.plot("a", "b", type: "line")
    assert_plot_type "column", df.plot("a", "b", type: "column")
    assert_plot_type "bar", df.plot("a", "b", type: "bar")
    assert_plot_type "area", df.plot("a", "b", type: "area")
    assert_plot_type "scatter", df.plot("b", "b", type: "scatter")
  end

  def test_plot_type
    df = Polars::DataFrame.new({"a" => ["one", "two", "three"], "b" => [1, 2, 3]})
    assert_plot_type "pie", df.plot.pie("a", "b")
    assert_plot_type "line", df.plot.line("a", "b")
    assert_plot_type "column", df.plot.column("a", "b")
    assert_plot_type "bar", df.plot.bar("a", "b")
    assert_plot_type "area", df.plot.area("a", "b")
    assert_plot_type "scatter", df.plot.scatter("b", "b")
    assert_plot_type "scatter", df.plot.point("b", "b")
  end

  def test_color_option
    df = Polars::DataFrame.new({"a" => ["one", "two", "three"], "b" => [1, 2, 3], "c" => ["group1", "group1", "group2"]})
    assert_group df.plot.line("a", "b", color: "c")
    assert_group df.plot.column("a", "b", color: "c")
    assert_group df.plot.bar("a", "b", color: "c")
    assert_group df.plot.area("a", "b", color: "c")
    assert_group df.plot.scatter("b", "b", color: "c")
  end

  def test_type_unknown
    df = Polars::DataFrame.new({"a" => ["one", "two", "three"]})
    error = assert_raises do
      df.plot("a", "a")
    end
    assert_equal "Cannot determine type. Use the type option.", error.message
  end

  def test_series
    s = Polars::Series.new("a", [1, 2, 3])
    assert_plot_type "bar", s.plot.hist
    assert_plot_type "area", s.plot.kde
    assert_plot_type "line", s.plot.line
  end

  def assert_plot_type(expected, plot)
    assert_kind_of Vega::LiteChart, plot

    case expected
    when "column"
      assert_equal "bar", plot.spec[:mark][:type]
    when "pie"
      assert_equal "arc", plot.spec[:mark][:type]
    when "scatter"
      assert_equal "circle", plot.spec[:mark][:type]
    else
      assert_equal expected, plot.spec[:mark][:type]
    end
  end

  def assert_group(plot)
    assert_kind_of Vega::LiteChart, plot
    assert_equal "c", plot.spec[:encoding][:color][:field]
  end
end
