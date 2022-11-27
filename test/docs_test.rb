require_relative "test_helper"

class DocsTest < Minitest::Test
  def test_cat_expr
    assert_docs Polars::CatExpr, required: true
  end

  def test_data_frame
    assert_docs Polars::DataFrame, required: true
  end

  def test_date_time_expr
    assert_docs Polars::DateTimeExpr, required: true
  end

  def test_expr
    assert_docs Polars::Expr
  end

  def test_functions
    assert_docs Polars::Functions, required: true
  end

  def test_group_by
    assert_docs Polars::GroupBy, required: true
  end

  def test_io
    assert_docs Polars::IO, required: true
  end

  def test_lazy_frame
    assert_docs Polars::LazyFrame, required: true
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

  def test_meta_expr
    assert_docs Polars::MetaExpr, required: true
  end

  def test_series
    assert_docs Polars::Series, required: true
  end

  def test_string_expr
    assert_docs Polars::StringExpr
  end

  def test_struct_expr
    assert_docs Polars::StructExpr, required: true
  end

  def assert_docs(cls, required: false)
    @@once ||= begin
      require "yard"

      YARD.parse || true
    end

    in_temp_dir do
      P(cls.to_s).meths.each do |method|
        next if method.visibility != :public || method.tags(:private).any?

        if method.docstring.empty?
          if required
            raise "Missing docs (#{method.name})"
          else
            next
          end
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
    return if [:struct, :read_csv_batched, :field, :rename_fields, :unnest, :arange, :explode, :is_datelike].include?(method.name)

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
