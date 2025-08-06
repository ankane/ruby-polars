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

  def test_catalog
    assert_docs Polars::Catalog
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

  def test_data_type_expr
    assert_docs Polars::DataTypeExpr
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

  def test_schema
    assert_docs Polars::Schema
  end

  def test_selector
    assert_docs Polars::Selector
  end

  def test_selectors
    assert_docs Polars::Selectors
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
        next if method.namespace.to_s == cls.ancestors[1].to_s

        if method.docstring.empty?
          raise "Missing docs (#{method.name})"
        end

        assert_params(method)
        assert_return(method)
        assert_examples(method, cls)
      end
    end
  end

  def assert_params(method)
    actual = method.tags(:param).map(&:name)
    expected = method.parameters.map(&:first).map { |v| v.gsub("*", "").gsub(":", "") }.reject { |v| v.start_with?("&") }
    missing = expected - actual
    extra = actual - expected
    if missing.any? && actual.any?
      # puts "Missing @param tags (#{method}) #{missing}"
    end
    if extra.any? && expected.any?
      puts "Extra @param tags (#{method}) #{extra}"
    end
    if actual.length == expected.length && actual != expected
      puts "Unordered @param tags (#{method})"
      p actual
      p expected
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

    # requires nightly
    return if [:to_titlecase].include?(method.name)

    # yard global
    return if [:log].include?(method.name)

    if ENV["EXAMPLES"] && missing_examples?(method, cls)
      warn "Missing examples (#{method})"
    end

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
        next if [:sort, :mode, :duration, :_hash, :hash_rows, :flatten, :value_counts, :agg, :top_k, :bottom_k, :get_categories, :to_physical].include?(method.name)

        # check output
        lines = code.split("\n")
        if RUBY_VERSION.to_f >= 3.4
          output = output.gsub(" => ", "=>")
        end
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

  def missing_examples?(method, cls)
    method.tags(:example).empty? &&
    ![Polars::Config, Polars::IO, Polars::Testing, Polars::DataType, Polars::SQLContext].include?(cls) &&
    method.name.match?(/\A[a-z]/i) &&
    ![:inspect, :to_s, :plot, :list, :arr, :bin, :cat, :dt, :meta, :name, :str, :struct, :initialize, :set_random_seed, :col].include?(method.name) &&
    !method.name.start_with?("write_")
  end
end
