module Polars
  module IO
    # Register your IO plugin and initialize a LazyFrame.
    #
    # See the [user guide](https://docs.pola.rs/user-guide/plugins/io_plugins)
    # for more information about plugins.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @param io_source [Object]
    #   Function that accepts the following arguments:
    #   - with_columns
    #       Columns that are projected. The reader must
    #       project these columns if applied
    #   - predicate
    #       Polars expression. The reader must filter
    #       their rows accordingly.
    #   - n_rows
    #       Materialize only n rows from the source.
    #       The reader can stop when `n_rows` are read.
    #   - batch_size
    #       A hint of the ideal batch size the reader's
    #       generator must produce.
    #
    #   The function should return a an iterator/generator
    #   that produces DataFrames.
    # @param schema [Object]
    #   Schema or function that when called produces the schema that the reader
    #   will produce before projection pushdown.
    # @param validate_schema [Boolean]
    #   Whether the engine should validate if the batches generated match
    #   the given schema. It's an implementation error if this isn't
    #   the case and can lead to bugs that are hard to solve.
    # @param is_pure [Boolean]
    #   Whether the IO source is pure. Repeated occurrences of same IO source in
    #   a LazyFrame plan can be de-duplicated during optimization if they are
    #   pure.
    # @param explain_name [String]
    #   Custom label for the scan node in the query plan produced by `explain`.
    # @param explain_detail [String]
    #   Short, single-line detail appended under the scan header in the query plan
    #   produced by `explain` (for example, a summary of source configuration).
    #
    # @return [LazyFrame]
    def register_io_source(
      io_source,
      schema:,
      validate_schema: false,
      is_pure: false,
      explain_name: nil,
      explain_detail: nil
    )
      raise Todo
    end
  end
end
