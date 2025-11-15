module Polars
  module Convert
    # Construct a DataFrame from a hash of arrays.
    #
    # This operation clones data, unless you pass in a `Hash<String, Series>`.
    #
    # @param data [Hash]
    #   Two-dimensional data represented as a hash. Hash must contain
    #   arrays.
    # @param schema [Object]
    #   The DataFrame schema may be declared in several ways:
    #
    #   * As a hash of \\\\{name:type} pairs; if type is nil, it will be auto-inferred.
    #   * As an array of column names; in this case types are automatically inferred.
    #   * As an array of [name,type] pairs; this is equivalent to the hash form.
    #
    #   If you supply an array of column names that does not match the names in the
    #   underlying data, the names given here will overwrite them. The number
    #   of names given in the schema should match the underlying data dimensions.
    # @param schema_overrides [Hash]
    #   Support type specification or override of one or more columns; note that
    #   any dtypes inferred from the columns param will be overridden.
    # @param strict [Boolean]
    #   Throw an error if any `data` value does not exactly match the given or inferred
    #   data type for that column. If set to `false`, values that do not match the data
    #   type are cast to that data type or, if casting is not possible, set to null
    #   instead.
    #
    # @return [DataFrame]
    #
    # @example
    #   data = {"a" => [1, 2], "b" => [3, 4]}
    #   Polars.from_hash(data)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 3   │
    #   # │ 2   ┆ 4   │
    #   # └─────┴─────┘
    def from_hash(data, schema: nil, schema_overrides: nil, strict: true)
      Utils.wrap_df(
        DataFrame.hash_to_rbdf(
          data,
          schema: schema,
          schema_overrides: schema_overrides,
          strict: strict
        )
      )
    end

    # Construct a DataFrame from a sequence of dictionaries. This operation clones data.
    #
    # @param data [Array]
    #   Sequence with dictionaries mapping column name to value
    # @param schema [Object]
    #   The DataFrame schema may be declared in several ways:
    #
    #   * As a dict of \\\\{name:type} pairs; if type is None, it will be auto-inferred.
    #   * As a list of column names; in this case types are automatically inferred.
    #   * As a list of (name,type) pairs; this is equivalent to the dictionary form.
    #
    #   If a list of column names is supplied that does NOT match the names in the
    #   underlying data, the names given here will overwrite the actual fields in
    #   the order that they appear - however, in this case it is typically clearer
    #   to rename after loading the frame.
    #
    #   If you want to drop some of the fields found in the input dictionaries, a
    #   *partial* schema can be declared, in which case omitted fields will not be
    #   loaded. Similarly, you can extend the loaded frame with empty columns by
    #   adding them to the schema.
    # @param schema_overrides [Hash]
    #   Support override of inferred types for one or more columns.
    # @param strict [Boolean]
    #   Throw an error if any `data` value does not exactly match the given or inferred
    #   data type for that column. If set to `false`, values that do not match the data
    #   type are cast to that data type or, if casting is not possible, set to null
    #   instead.
    # @param infer_schema_length [Integer]
    #   The maximum number of rows to scan for schema inference.
    #   If set to `nil`, the full data may be scanned *(this is slow)*.
    #
    # @return [DataFrame]
    #
    # @example
    #   data = [{"a" => 1, "b" => 4}, {"a" => 2, "b" => 5}, {"a" => 3, "b" => 6}]
    #   Polars.from_hashes(data)
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 4   │
    #   # │ 2   ┆ 5   │
    #   # │ 3   ┆ 6   │
    #   # └─────┴─────┘
    #
    # @example Declaring a partial `schema` will drop the omitted columns.
    #   Polars.from_hashes(data, schema: {"a" => Polars::Int32})
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i32 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 2   │
    #   # │ 3   │
    #   # └─────┘
    def from_hashes(
      data,
      schema: nil,
      schema_overrides: nil,
      strict: true,
      infer_schema_length: N_INFER_DEFAULT
    )
      if !data.any? && !(schema.any? || schema_overrides.any?)
        msg = "no data, cannot infer schema"
        raise NoDataError, msg
      end

      DataFrame.new(
        data,
        schema: schema,
        schema_overrides: schema_overrides,
        strict: strict,
        infer_schema_length: infer_schema_length
      )
    end

    # def from_records
    # end
  end
end
