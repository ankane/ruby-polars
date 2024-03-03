module Polars
  module Testing
    class AssertionError < StandardError; end

    def assert_frame_equal(
      left,
      right,
      check_row_order: true,
      check_column_order: true,
      check_dtype: true,
      check_exact: false,
      rtol: 1e-5,
      atol: 1e-8,
      categorical_as_str: false
    )
      lazy = _assert_correct_input_type(left, right)
      objects = lazy ? "LazyFrames" : "DataFrames"

      _assert_frame_schema_equal(
        left,
        right,
        check_column_order: check_column_order,
        check_dtype: check_dtype,
        objects: objects,
      )

      if lazy
        left, right = left.collect, right.collect
      end

      if left.height != right.height
        raise_assertion_error(
          objects, "number of rows does not match", left.height, right.height
        )
      end

      if !check_row_order
        left, right = _sort_dataframes(left, right)
      end

      left.columns.each do |c|
        s_left, s_right = left.get_column(c), right.get_column(c)
        begin
          _assert_series_values_equal(
            s_left,
            s_right,
            check_exact: check_exact,
            rtol: rtol,
            atol: atol,
            categorical_as_str: categorical_as_str
          )
        rescue AssertionError => exc
          raise_assertion_error(
            objects,
            "value mismatch for column #{c.inspect}",
            s_left.to_a,
            s_right.to_a,
            cause: exc
          )
        end
      end
    end

    def assert_series_equal(
      left,
      right,
      check_dtype: true,
      check_names: true,
      check_exact: false,
      rtol: 1e-5,
      atol: 1e-8,
      categorical_as_str: false
    )
      if !(left.is_a?(Series) && right.is_a?(Series))
        raise_assertion_error(
          "inputs",
          "unexpected input types",
          left.class.name,
          right.class.name
        )
      end

      if left.len != right.len
        raise_assertion_error("Series", "length mismatch", left.len, right.len)
      end

      if check_names && left.name != right.name
        raise_assertion_error("Series", "name mismatch", left.name, right.name)
      end

      if check_dtype && left.dtype != right.dtype
        raise_assertion_error("Series", "dtype mismatch", left.dtype, right.dtype)
      end

      _assert_series_values_equal(
        left,
        right,
        check_exact: check_exact,
        rtol: rtol,
        atol: atol,
        categorical_as_str: categorical_as_str
      )
    end

    private

    def _assert_correct_input_type(left, right)
      if left.is_a?(DataFrame) && right.is_a?(DataFrame)
        return false
      elsif left.is_a?(LazyFrame) && right.is_a?(DataFrame)
        return true
      else
        raise_assertion_error(
          "inputs",
          "unexpected input types",
          left.class.name,
          right.class.name
        )
      end
    end

    def _assert_frame_schema_equal(
      left,
      right,
      check_dtype:,
      check_column_order:,
      objects:
    )
      left_schema, right_schema = left.schema, right.schema

      # Fast path for equal frames
      if left_schema == right_schema
        return
      end

      # Special error message for when column names do not match
      if left_schema.keys != right_schema.keys
        if (left_not_right = right_schema.keys - left_schema.keys).any?
          msg = "columns #{left_not_right.inspect} in left #{objects[..-1]}, but not in right"
          raise msg
        else
          right_not_left = right_schema.keys - left_schema.keys
          msg = "columns #{right_not_left.inspect} in right #{objects[..-1]}, but not in left"
          raise msg
        end
      end

      if check_column_order
        left_columns, right_columns = left_schema.keys, right_schema.keys
        if left_columns != right_columns
          detail = "columns are not in the same order"
          raise_assertion_error(objects, detail, left_columns, right_columns)
        end
      end

      if check_dtype
        left_schema_dict, right_schema_dict = left_schema.to_h, right_schema.to_h
        if check_column_order || left_schema_dict != right_schema_dict
          detail = "dtypes do not match"
          raise_assertion_error(objects, detail, left_schema_dict, right_schema_dict)
        end
      end
    end

    def _sort_dataframes(left, right)
      by = left.columns
      begin
        left = left.sort(by)
        right = right.sort(by)
      rescue ComputeError
        msg = "cannot set `check_row_order: false` on frame with unsortable columns"
        raise msg
      end
      [left, right]
    end

    def _assert_series_values_equal(
      left,
      right,
      check_exact:,
      rtol:,
      atol:,
      categorical_as_str:
    )
      if categorical_as_str
        if left.dtype == Categorical
          left = left.cast(String)
        end
        if right.dtype == Categorical
          right = right.cast(String)
        end
      end

      # Determine unequal elements
      begin
        unequal = left.ne_missing(right)
      rescue ComputeError => exc
        raise_assertion_error(
          "Series",
          "incompatible data types",
          left.dtype,
          right.dtype,
          cause: exc
        )
      end

      # Check nested dtypes in separate function
      if _comparing_nested_floats(left.dtype, right.dtype)
        begin
          _assert_series_nested_values_equal(
            left: left.filter(unequal),
            right: right.filter(unequal),
            check_exact: check_exact,
            rtol: rtol,
            atol: atol,
            categorical_as_str: categorical_as_str
          )
        rescue AssertionError => exc
          raise_assertion_error(
            "Series",
            "nested value mismatch",
            left.to_a,
            right.to_a,
            cause: exc
          )
        else
          return
        end
      end

      # If no differences found during exact checking, we're done
      if !unequal.any
        return
      end

      # Only do inexact checking for float types
      if check_exact || !left.dtype.is_float || !right.dtype.is_float
        raise_assertion_error(
          "Series", "exact value mismatch", left.to_a, right.to_a
        )
      end

      _assert_series_null_values_match(left, right)
      _assert_series_nan_values_match(left, right)
      _assert_series_values_within_tolerance(
        left,
        right,
        unequal,
        rtol: rtol,
        atol: atol
      )
    end

    def _assert_series_nested_values_equal(
      left,
      right,
      check_exact:,
      rtol:,
      atol:,
      categorical_as_str:
    )
      # compare nested lists element-wise
      if _comparing_lists(left.dtype, right.dtype)
        left.right.zip do |s1, s2|
          if s1.nil? || s2.nil?
            raise_assertion_error("Series", "nested value mismatch", s1, s2)
          end

          _assert_series_values_equal(
            s1,
            s2,
            check_exact: check_exact,
            rtol: rtol,
            atol: atol,
            categorical_as_str: categorical_as_str
          )
        end

      # unnest structs as series and compare
      else
        ls, rs = left.struct.unnest, right.struct.unnest
        ls.zip(rs) do |s1, s2|
          _assert_series_values_equal(
            s1,
            s2,
            check_exact: check_exact,
            rtol: rtol,
            atol: atol,
            categorical_as_str: categorical_as_str
          )
        end
      end
    end

    def _assert_series_null_values_match(left, right)
      null_value_mismatch = left.is_null != right.is_null
      if null_value_mismatch.any
        raise_assertion_error(
          "Series", "null value mismatch", left.to_a, right.to_a
        )
      end
    end

    def _assert_series_nan_values_match(left, right)
      if !_comparing_floats(left.dtype, right.dtype)
        return
      end
      nan_value_mismatch = left.is_nan != right.is_nan
      if nan_value_mismatch.any
        raise_assertion_error(
          "Series",
          "nan value mismatch",
          left.to_a,
          right.to_a
        )
      end
    end

    def _comparing_floats(left, right)
      left.is_float && right.is_float
    end

    def _comparing_lists(left, right)
      [List, Array].include?(left) && [List, Array].include?(right)
    end

    def _comparing_structs(left, right)
      left == Struct && right == Struct
    end

    def _comparing_nested_floats(left, right)
      if !(_comparing_lists(left, right) || _comparing_structs(left, right))
        return false
      end

      (FLOAT_DTYPES & unpack_dtypes(left)) && (FLOAT_DTYPES & unpack_dtypes(right))
    end

    def raise_assertion_error(objects, detail, left, right, cause: nil)
      msg = "#{objects} are different (#{detail})\n[left]: #{left}\n[right]: #{right}"
      raise AssertionError, msg
    end
  end
end
