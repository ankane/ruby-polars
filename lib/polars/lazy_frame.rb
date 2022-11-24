module Polars
  class LazyFrame
    attr_accessor :_ldf

    def self._from_rbldf(rb_ldf)
      ldf = LazyFrame.allocate
      ldf._ldf = rb_ldf
      ldf
    end

    def self._scan_csv(
      file,
      has_header: true,
      sep: ",",
      comment_char: nil,
      quote_char: '"',
      skip_rows: 0,
      dtypes: nil,
      null_values: nil,
      ignore_errors: false,
      cache: true,
      with_column_names: nil,
      infer_schema_length: 100,
      n_rows: nil,
      encoding: "utf8",
      low_memory: false,
      rechunk: true,
      skip_rows_after_header: 0,
      row_count_name: nil,
      row_count_offset: 0,
      parse_dates: false,
      eol_char: "\n"
    )
      dtype_list = nil
      if !dtypes.nil?
        dtype_list = []
        dtypes.each do |k, v|
          dtype_list << [k, Utils.rb_type_to_dtype(v)]
        end
      end
      processed_null_values = Utils._process_null_values(null_values)

      _from_rbldf(
        RbLazyFrame.new_from_csv(
          file,
          sep,
          has_header,
          ignore_errors,
          skip_rows,
          n_rows,
          cache,
          dtype_list,
          low_memory,
          comment_char,
          quote_char,
          processed_null_values,
          infer_schema_length,
          with_column_names,
          rechunk,
          skip_rows_after_header,
          encoding,
          Utils._prepare_row_count_args(row_count_name, row_count_offset),
          parse_dates,
          eol_char
        )
      )
    end

    def self._scan_parquet(
      file,
      n_rows: nil,
      cache: true,
      parallel: "auto",
      rechunk: true,
      row_count_name: nil,
      row_count_offset: 0,
      storage_options: nil,
      low_memory: false
    )
      _from_rbldf(
        RbLazyFrame.new_from_parquet(
          file,
          n_rows,
          cache,
          parallel,
          rechunk,
          Utils._prepare_row_count_args(row_count_name, row_count_offset),
          low_memory
        )
      )
    end

    # def self._scan_ipc
    # end

    def self._scan_ndjson(
      file,
      infer_schema_length: nil,
      batch_size: nil,
      n_rows: nil,
      low_memory: false,
      rechunk: true,
      row_count_name: nil,
      row_count_offset: 0
    )
      _from_rbldf(
        RbLazyFrame.new_from_ndjson(
          file,
          infer_schema_length,
          batch_size,
          n_rows,
          low_memory,
          rechunk,
          Utils._prepare_row_count_args(row_count_name, row_count_offset)
        )
      )
    end

    # def self.from_json
    # end

    # def self.read_json
    # end

    # def columns
    # end

    # def dtypes
    # end

    # def schema
    # end

    # def width
    # end

    # def include?(key)
    # end

    # clone handled by initialize_copy

    # def [](item)
    # end

    # def to_s
    # end
    # alias_method :inspect, :to_s

    # def write_json
    # end

    # def pipe
    # end

    # def describe_plan
    # end

    # def describe_optimized_plan
    # end

    # def show_graph
    # end

    # def sort
    # end

    # def profile
    # end

    def collect(
      type_coercion: true,
      predicate_pushdown: true,
      projection_pushdown: true,
      simplify_expression: true,
      string_cache: false,
      no_optimization: false,
      slice_pushdown: true,
      common_subplan_elimination: true,
      allow_streaming: false
    )
      if no_optimization
        predicate_pushdown = false
        projection_pushdown = false
        slice_pushdown = false
        common_subplan_elimination = false
      end

      if allow_streaming
        common_subplan_elimination = false
      end

      ldf = _ldf.optimization_toggle(
        type_coercion,
        predicate_pushdown,
        projection_pushdown,
        simplify_expression,
        slice_pushdown,
        common_subplan_elimination,
        allow_streaming
      )
      Utils.wrap_df(ldf.collect)
    end

    # def fetch
    # end

    def lazy
      self
    end

    # def cache
    # end

    # def cleared
    # end

    def filter(predicate)
      _from_rbldf(
        _ldf.filter(
          Utils.expr_to_lit_or_expr(predicate, str_to_lit: false)._rbexpr
        )
      )
    end

    def select(exprs)
      exprs = Utils.selection_to_rbexpr_list(exprs)
      _from_rbldf(_ldf.select(exprs))
    end

    def groupby(by, maintain_order: false)
      rbexprs_by = Utils.selection_to_rbexpr_list(by)
      lgb = _ldf.groupby(rbexprs_by, maintain_order)
      LazyGroupBy.new(lgb, self.class)
    end

    # def groupby_rolling
    # end

    # def groupby_dynamic
    # end

    # def join_asof
    # end

    def join(
      other,
      left_on: nil,
      right_on: nil,
      on: nil,
      how: "inner",
      suffix: "_right",
      allow_parallel: true,
      force_parallel: false
    )
      if !other.is_a?(LazyFrame)
        raise ArgumentError, "Expected a `LazyFrame` as join table, got #{other.class.name}"
      end

      if how == "cross"
        return _from_rbldf(
          _ldf.join(
            other._ldf, [], [], allow_parallel, force_parallel, how, suffix
          )
        )
      end

      if !on.nil?
        rbexprs = Utils.selection_to_rbexpr_list(on)
        rbexprs_left = rbexprs
        rbexprs_right = rbexprs
      elsif !left_on.nil? && !right_on.nil?
        rbexprs_left = Utils.selection_to_rbexpr_list(left_on)
        rbexprs_right = Utils.selection_to_rbexpr_list(right_on)
      else
        raise ArgumentError, "must specify `on` OR `left_on` and `right_on`"
      end

      _from_rbldf(
        self._ldf.join(
          other._ldf,
          rbexprs_left,
          rbexprs_right,
          allow_parallel,
          force_parallel,
          how,
          suffix,
        )
      )
    end

    def with_columns(exprs)
      exprs =
        if exprs.nil?
          []
        elsif exprs.is_a?(Expr)
          [exprs]
        else
          exprs.to_a
        end

      rbexprs = []
      exprs.each do |e|
        case e
        when Expr
          rbexprs << e._rbexpr
        when Series
          rbexprs = Utils.lit(e)._rbexpr
        else
          raise ArgumentError, "Expected an expression, got #{e}"
        end
      end

      _from_rbldf(_ldf.with_columns(rbexprs))
    end

    # def with_context
    # end

    def with_column(column)
      with_columns([column])
    end

    # def drop
    # end

    def rename(mapping)
      existing = mapping.keys
      _new = mapping.values
      _from_rbldf(_ldf.rename(existing, _new))
    end

    # def reverse
    # end

    # def shift
    # end

    # def shift_and_fill
    # end

    # def slice
    # end

    # def limit
    # end

    # def head
    # end

    # def tail
    # end

    # def last
    # end

    # def first
    # end

    # def with_row_count
    # end

    # def take_every
    # end

    # def fill_null
    # end

    def fill_nan(fill_value)
      if !fill_value.is_a?(Expr)
        fill_value = Utils.lit(fill_value)
      end
      _from_rbldf(_ldf.fill_nan(fill_value._rbexpr))
    end

    # def std
    # end

    # def var
    # end

    # def max
    # end

    # def min
    # end

    # def sum
    # end

    # def mean
    # end

    # def median
    # end

    # def quantile
    # end

    def explode(columns)
      columns = Utils.selection_to_rbexpr_list(columns)
      _from_rbldf(_ldf.explode(columns))
    end

    # def unique
    # end

    # def drop_nulls
    # end

    # def melt
    # end

    # def map
    # end

    # def interpolate
    # end

    # def unnest
    # end

    private

    def initialize_copy(other)
      super
      self._ldf = _ldf._clone
    end

    def _from_rbldf(rb_ldf)
      self.class._from_rbldf(rb_ldf)
    end
  end
end
