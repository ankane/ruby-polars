require_relative "test_helper"

class DocsTest < Minitest::Test
  def setup
    super unless stress?
  end

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

  def test_data_frame_plot
    assert_docs Polars::DataFramePlot
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

  def test_dynamic_group_by
    assert_docs Polars::DynamicGroupBy
  end

  def test_expr
    assert_docs Polars::Expr
  end

  def test_extension_expr
    assert_docs Polars::ExtensionExpr
  end

  def test_extension_name_space
    assert_docs Polars::ExtensionNameSpace
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

  def test_rolling_group_by
    assert_docs Polars::RollingGroupBy
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

  def test_series_plot
    assert_docs Polars::SeriesPlot
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

      with_stress(false) do
        YARD.parse
      end
      true
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

    # TODO fix
    return if [:combine].include?(method.name)

    # TODO fix
    return if cls == Polars::Expr && [:pipe, :inspect_].include?(method.name)

    # todos
    todo = [:cum_reduce, :reduce, :show_graph, :from_numo].include?(method.name)
    todo |= cls == Polars::NameExpr && method.name == :map
    todo |= cls == Polars::Functions && [:fold, :cum_fold, :cum_sum_horizontal].include?(method.name)
    todo |= cls == Polars::Expr && method.name == :deserialize
    todo |= cls == Polars::LazyFrame && [:deserialize, :serialize, :map_batches].include?(method.name)
    todo |= cls == Polars::MetaExpr && method.name == :serialize
    todo |= cls == Polars::LazyGroupBy && method.name == :map_groups

    if missing_examples?(method, cls)
      warn "Missing examples (#{method})"
    end

    # TODO remove
    if cls == Polars::DateTimeExpr
      require "active_support/core_ext/date_time"
    elsif cls == Polars::DateTimeNameSpace
      require "active_support/core_ext/date"
    end

    puts "#{cls}##{method.name}" if stress?

    code = ""
    method.tags(:example).each do |example|
      # use variables from previous examples
      code += "\n" + example.text
      # suppress warning about void context
      code = "_ = (\n#{code}\n)"
      begin
        # just final output
        output =
          with_stress(method.name != :estimated_size) do
            if cls == Polars::Config
              capture_io { instance_eval(code) }[0].chomp
            elsif method.tags(:deprecated).any?
              # TODO improve
              out = nil
              capture_io { out = instance_eval(code).inspect }
              out
            elsif todo
              assert_raises(Polars::Todo) do
                instance_eval(code)
              end
              ""
            else
              instance_eval(code).inspect
            end
          end

        # print output
        if ENV["VERBOSE"]
          puts method.name
          puts output
          puts
        end

        # non-deterministic output
        next if [:sort, :mode, :duration, :hash_, :hash_rows, :flatten, :value_counts, :agg, :top_k, :bottom_k, :get_categories, :to_physical, :profile, :having, :map_groups, :group_by].include?(method.name)

        next if [Polars::GroupBy, Polars::LazyGroupBy].include?(cls) && [:len].include?(method.name)

        # check output
        lines = code.delete_suffix("\n)").split("\n")
        if RUBY_VERSION.to_f >= 3.4
          output = output.gsub(" => ", "=>")
        end
        if todo
          # do nothing
        elsif lines.last.start_with?("# => ")
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
        elsif !([:initialize, :lit, :plot, :enable_string_cache, :disable_string_cache, :tree_format, :explain, :lazy].include?(method.name) || method.name.start_with?("write_") || cls == Polars::Selectors || lines.last.include?(" = "))
          warn "Missing example output (#{cls}##{method.name})"
        end
      rescue => e
        raise e if ENV["DEBUG"]
        raise "Example failed (#{method.name}): #{e.message}"
      end
    end
  end

  def missing_examples?(method, cls)
    method.tags(:example).empty? &&
    ![Polars::Config, Polars::IO, Polars::Testing, Polars::DataType, Polars::SQLContext, Polars::DataFramePlot, Polars::SeriesPlot, Polars::Catalog, Polars::DynamicGroupBy, Polars::RollingGroupBy, Polars::ExtensionExpr, Polars::ExtensionNameSpace].include?(cls) &&
    method.name.match?(/\A[a-z]/i) &&
    ![:inspect, :plot, :list, :arr, :bin, :cat, :dt, :meta, :name, :str, :struct, :ext, :initialize, :set_random_seed, :col, :select_seq, :with_columns_seq, :eq, :ne, :gt, :ge, :lt, :le, :shrink_to_fit, :flags, :set_sorted, :each, :each_row, :rechunk, :first, :last, :approx_n_unique, :forward_fill, :backward_fill, :repeat_by, :rolling_rank_by, :cache, :is_local, :split, :rolling_cov, :rolling_corr, :escape_regex, :using_string_cache, :quantile, :groups, :collect_all, :collect_batches, :as_expression, :as_selector, :datetime, :time, :duration, :dtype_of, :self_dtype].include?(method.name) &&
    !method.name.start_with?("write_") &&
    !method.name.start_with?("bitwise_") &&
    !method.name.start_with?("to_") &&
    !method.name.end_with?("?") &&
    !method.name.end_with?("!")
  end
end
