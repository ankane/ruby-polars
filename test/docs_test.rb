require_relative "test_helper"

class DocsTest < Minitest::Test
  def test_cat_expr
    assert_docs Polars::CatExpr
  end

  def test_cat_name_space
    assert_docs Polars::CatNameSpace
  end

  def test_data_frame
    assert_docs Polars::DataFrame
  end

  def test_date_time_expr
    assert_docs Polars::DateTimeExpr
  end

  def test_date_time_name_space
    assert_docs Polars::DateTimeNameSpace
  end

  def test_expr
    assert_docs Polars::Expr
  end

  def test_functions
    assert_docs Polars::Functions
  end

  def test_group_by
    assert_docs Polars::GroupBy
  end

  def test_io
    assert_docs Polars::IO
  end

  def test_lazy_frame
    assert_docs Polars::LazyFrame
  end

  def test_lazy_functions
    assert_docs Polars::LazyFunctions
  end

  def test_lazy_group_by
    assert_docs Polars::LazyGroupBy
  end

  def test_list_expr
    assert_docs Polars::ListExpr
  end

  def test_list_name_space
    assert_docs Polars::ListNameSpace
  end

  def test_meta_expr
    assert_docs Polars::MetaExpr
  end

  def test_series
    assert_docs Polars::Series
  end

  def test_string_expr
    assert_docs Polars::StringExpr
  end

  def test_string_name_space
    assert_docs Polars::StringNameSpace
  end

  def test_struct_expr
    assert_docs Polars::StructExpr
  end

  def test_struct_name_space
    assert_docs Polars::StructNameSpace
  end

  def assert_docs(cls)
    @@once ||= begin
      require "yard"

      YARD.parse || true
    end

    in_temp_dir do
      P(cls.to_s).meths.each do |method|
        next if method.visibility != :public || method.tags(:private).any?

        if method.docstring.empty?
          raise "Missing docs (#{method.name})"
        end

        assert_return(method)
        assert_examples(method)
      end
    end
  end

  def assert_return(method)
    if method.tags(:return).empty?
      raise "Missing @return tag (#{method.name})"
    end
  end

  def assert_examples(method)
    return if [:from_epoch, :coalesce, :cumsum, :arange, :concat, :lengths, :is_nan, :join, :is_not_nan, :read_csv_batched].include?(method.name)

    code = ""
    method.tags(:example).each do |example|
      # use variables from previous examples
      code += "\n" + example.text
      begin
        # just final output
        output = instance_eval(code)

        # print output
        if ENV["VERBOSE"]
          puts method.name
          p output
          puts
        end

        next if [:sort, :sample, :mode].include?(method.name)

        # check output
        lines = code.split("\n")
        if lines.last.start_with?("# => ")
          expected = lines.last[5..]
          assert_equal expected, output.inspect, "Example output (#{method.name})"
        elsif lines.last.start_with?("# ")
          expected = []
          while (line = lines.pop) && line != "# =>"
            expected << line[2..]
          end
          expected = expected.reverse.join("\n")
          output = output.inspect.gsub("\t", "        ")
          assert_equal expected, output, "Example output (#{method.name})"
        end
      rescue => e
        raise "Example failed (#{method.name}): #{e.message}"
      end
    end
  end
end
