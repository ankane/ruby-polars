module Polars
  module Testing
    # Assert that the left and right frame are equal.
    #
    # Raises a detailed `AssertionError` if the frames differ.
    # This function is intended for use in unit tests.
    #
    # @param left [Object]
    #   The first DataFrame or LazyFrame to compare.
    # @param right [Object]
    #   The second DataFrame or LazyFrame to compare.
    # @param check_row_order [Boolean]
    #   Require row order to match.
    # @param check_column_order [Boolean]
    #   Require column order to match.
    # @param check_dtype [Boolean]
    #   Require data types to match.
    # @param check_exact [Boolean]
    #   Require float values to match exactly. If set to `false`, values are considered
    #   equal when within tolerance of each other (see `rtol` and `atol`).
    #   Only affects columns with a Float data type.
    # @param rtol [Float]
    #   Relative tolerance for inexact checking. Fraction of values in `right`.
    # @param atol [Float]
    #   Absolute tolerance for inexact checking.
    # @param categorical_as_str [Boolean]
    #   Cast categorical columns to string before comparing. Enabling this helps
    #   compare columns that do not share the same string cache.
    #
    # @return [nil]
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

      if lazy
        left, right = left.collect, right.collect
      end

      Plr.assert_dataframe_equal_rb(
        left._df,
        right._df,
        check_row_order,
        check_column_order,
        check_dtype,
        check_exact,
        rtol,
        atol,
        categorical_as_str,
      )
    end

    # Assert that the left and right frame are **not** equal.
    #
    # This function is intended for use in unit tests.
    #
    # @param left [Object]
    #   The first DataFrame or LazyFrame to compare.
    # @param right [Object]
    #   The second DataFrame or LazyFrame to compare.
    # @param check_row_order [Boolean]
    #   Require row order to match.
    # @param check_column_order [Boolean]
    #   Require column order to match.
    # @param check_dtype [Boolean]
    #   Require data types to match.
    # @param check_exact [Boolean]
    #   Require float values to match exactly. If set to `false`, values are considered
    #   equal when within tolerance of each other (see `rtol` and `atol`).
    #   Only affects columns with a Float data type.
    # @param rtol [Float]
    #   Relative tolerance for inexact checking. Fraction of values in `right`.
    # @param atol [Float]
    #   Absolute tolerance for inexact checking.
    # @param categorical_as_str [Boolean]
    #   Cast categorical columns to string before comparing. Enabling this helps
    #   compare columns that do not share the same string cache.
    #
    # @return [nil]
    def assert_frame_not_equal(
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
      begin
        assert_frame_equal(
          left,
          right,
          check_column_order: check_column_order,
          check_row_order: check_row_order,
          check_dtype: check_dtype,
          check_exact: check_exact,
          rtol: rtol,
          atol: atol,
          categorical_as_str: categorical_as_str
        )
      rescue AssertionError
        return
      end

      msg = "frames are equal"
      raise AssertionError, msg
    end

    # Assert that the left and right Series are equal.
    #
    # Raises a detailed `AssertionError` if the Series differ.
    # This function is intended for use in unit tests.
    #
    # @param left [Object]
    #   The first Series to compare.
    # @param right [Object]
    #   The second Series to compare.
    # @param check_dtype [Boolean]
    #   Require data types to match.
    # @param check_names [Boolean]
    #   Require names to match.
    # @param check_order [Boolean]
    #   Requires elements to appear in the same order.
    # @param check_exact [Boolean]
    #   Require float values to match exactly. If set to `false`, values are considered
    #   equal when within tolerance of each other (see `rtol` and `atol`).
    #   Only affects columns with a Float data type.
    # @param rtol [Float]
    #   Relative tolerance for inexact checking, given as a fraction of the values in
    #   `right`.
    # @param atol [Float]
    #   Absolute tolerance for inexact checking.
    # @param categorical_as_str [Boolean]
    #   Cast categorical columns to string before comparing. Enabling this helps
    #   compare columns that do not share the same string cache.
    #
    # @return [nil]
    def assert_series_equal(
      left,
      right,
      check_dtype: true,
      check_names: true,
      check_order: true,
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

      Plr.assert_series_equal_rb(
        left._s,
        right._s,
        check_dtype,
        check_names,
        check_order,
        check_exact,
        rtol,
        atol,
        categorical_as_str
      )
    end

    # Assert that the left and right Series are **not** equal.
    #
    # This function is intended for use in unit tests.
    #
    # @param left [Object]
    #   The first Series to compare.
    # @param right [Object]
    #   The second Series to compare.
    # @param check_dtype [Boolean]
    #   Require data types to match.
    # @param check_names [Boolean]
    #   Require names to match.
    # @param check_exact [Boolean]
    #   Require float values to match exactly. If set to `false`, values are considered
    #   equal when within tolerance of each other (see `rtol` and `atol`).
    #   Only affects columns with a Float data type.
    # @param rtol [Float]
    #   Relative tolerance for inexact checking, given as a fraction of the values in
    #   `right`.
    # @param atol [Float]
    #   Absolute tolerance for inexact checking.
    # @param categorical_as_str [Boolean]
    #   Cast categorical columns to string before comparing. Enabling this helps
    #   compare columns that do not share the same string cache.
    #
    # @return [nil]
    def assert_series_not_equal(
      left,
      right,
      check_dtype: true,
      check_names: true,
      check_exact: false,
      rtol: 1e-5,
      atol: 1e-8,
      categorical_as_str: false
    )
      begin
        assert_series_equal(
          left,
          right,
          check_dtype: check_dtype,
          check_names: check_names,
          check_exact: check_exact,
          rtol: rtol,
          atol: atol,
          categorical_as_str: categorical_as_str
        )
      rescue AssertionError
        return
      end

      msg = "Series are equal"
      raise AssertionError, msg
    end

    private

    def _assert_correct_input_type(left, right)
      if left.is_a?(DataFrame) && right.is_a?(DataFrame)
        false
      elsif left.is_a?(LazyFrame) && right.is_a?(DataFrame)
        true
      else
        raise_assertion_error(
          "inputs",
          "unexpected input types",
          left.class.name,
          right.class.name
        )
      end
    end

    def raise_assertion_error(objects, detail, left, right)
      msg = "#{objects} are different (#{detail})\n[left]:  #{left}\n[right]: #{right}"
      raise AssertionError, msg
    end
  end
end
