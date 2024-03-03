require_relative "test_helper"

class DocsTest < Minitest::Test
  def test_array_expr
    assert_docs Polars::ArrayExpr
  end

  def test_array_name_space
    assert_docs Polars::ArrayNameSpace
  end

  def test_binary_expr
    assert_docs Polars::BinaryExpr
  end

  def test_binary_name_space
    assert_docs Polars::BinaryNameSpace
  end

  def test_cat_expr
    assert_docs Polars::CatExpr
  end

  def test_cat_name_space
    assert_docs Polars::CatNameSpace
  end

  def test_config
    assert_docs Polars::Config
  end

  def test_convert
    assert_docs Polars::Convert
  end

  def test_data_frame
    assert_docs Polars::DataFrame
  end

  def test_data_types
    assert_docs Polars::DataType
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

  def test_name_expr
    assert_docs Polars::NameExpr
  end

  def test_series
    assert_docs Polars::Series
  end

  def test_sql_context
    assert_docs Polars::SQLContext
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

  def test_testing
    assert_docs Polars::Testing
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
        assert_examples(method, cls)
      end
    end
  end

  def assert_return(method)
    if method.tags(:return).empty?
      raise "Missing @return tag (#{method.name})"
    end
  end

  def assert_examples(method, cls)
    # requires files
    return if [:read_csv_batched, :sink_parquet, :sink_ipc, :sink_csv, :sink_ndjson].include?(method.name)

    # TODO fix
    return if [:align_frames, :coalesce, :cum_sum_horizontal, :cumsum_horizontal, :to_titlecase].include?(method.name)

    code = ""
    method.tags(:example).each do |example|
      # use variables from previous examples
      code += "\n" + example.text
      begin
        # just final output
        output =
          if cls == Polars::Config
            capture_io { instance_eval(code) }[0].chomp
          else
            instance_eval(code).inspect
          end

        # print output
        if ENV["VERBOSE"]
          puts method.name
          puts output
          puts
        end

        # non-deterministic output
        next if [:sort, :mode, :duration, :_hash, :hash_rows, :flatten, :value_counts].include?(method.name)

        # check output
        lines = code.split("\n")
        if lines.last.start_with?("# => ")
          expected = lines.last[5..]
          assert_equal expected, output, "Example output (#{method.name})"
        elsif lines.last.start_with?("# ")
          expected = []
          while (line = lines.pop) && line != "# =>"
            expected << line[2..]
          end
          expected = expected.reverse.join("\n")
          output = output.gsub("\t", "        ")
          assert_equal expected, output, "Example output (#{method.name})"
        end
      rescue => e
        raise e if ENV["DEBUG"]
        raise "Example failed (#{method.name}): #{e.message}"
      end
    end
  end
end
