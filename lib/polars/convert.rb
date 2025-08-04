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
    # @param columns [Array]
    #   Column labels to use for resulting DataFrame. If specified, overrides any
    #   labels already present in the data. Must match data dimensions.
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
    def from_hash(data, schema: nil, columns: nil)
      Utils.wrap_df(
        DataFrame.hash_to_rbdf(
          data,
          schema: schema || columns
        )
      )
    end

    # Construct a DataFrame from a sequence of dictionaries. This operation clones data.
    #
    # @param hashes [Array]
    #   Array with hashes mapping column name to value.
    # @param infer_schema_length [Integer]
    #   How many hashes/rows to scan to determine the data types
    #   if set to `nil` all rows are scanned. This will be slow.
    # @param schema [Object]
    #   Schema that (partially) overwrites the inferred schema.
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
    # @example Overwrite first column name and dtype
    #   Polars.from_hashes(data, schema: {"c" => :i32})
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ c   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i32 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 4   │
    #   # │ 2   ┆ 5   │
    #   # │ 3   ┆ 6   │
    #   # └─────┴─────┘
    #
    # @example Let polars infer the dtypes but inform about a 3rd column
    #   Polars.from_hashes(data, schema: {"a" => :unknown, "b" => :unknown, "c" => :i32})
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ a   ┆ b   ┆ c    │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ i64 ┆ i64 ┆ i32  │
    #   # ╞═════╪═════╪══════╡
    #   # │ 1   ┆ 4   ┆ null │
    #   # │ 2   ┆ 5   ┆ null │
    #   # │ 3   ┆ 6   ┆ null │
    #   # └─────┴─────┴──────┘
    # def from_hashes(hashes, infer_schema_length: 50, schema: nil)
    #   DataFrame._from_hashes(hashes, infer_schema_length: infer_schema_length, schema: schema)
    # end

    # def from_records
    # end
  end
end
