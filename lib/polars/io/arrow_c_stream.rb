module Polars
  module IO
    # Scan a source that implements the Arrow RbCapsule Interface.
    #
    # This creates a lazy scan over an object that exposes a `arrow_c_stream`
    # method (e.g. a `nanoarrow` stream). The
    # source is consumed, batch by batch, during query execution.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @param source [Object]
    #   Any object implementing the Arrow RbCapsule Interface
    #   (i.e. it has a `arrow_c_stream` method).
    #
    # @return [LazyFrame]
    #
    # @example
    #   schema = Nanoarrow.struct({"a" => Nanoarrow.int64, "b" => Nanoarrow.string})
    #   batches = [
    #     [{"a" => 1, "b" => "x"}, {"a" => 2, "b" => "y"}, {"a" => 3, "b" => "z"}],
    #     [{"a" => 4, "b" => "a"}, {"a" => 5, "b" => "b"}]
    #   ]
    #   reader = Nanoarrow::Array.from_chunks(batches, schema)
    #   Polars.scan_arrow_c_stream(reader).collect
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ str │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ x   │
    #   # │ 2   ┆ y   │
    #   # │ 3   ┆ z   │
    #   # │ 4   ┆ a   │
    #   # │ 5   ┆ b   │
    #   # └─────┴─────┘
    def scan_arrow_c_stream(source)
      if !source.respond_to?(:arrow_c_stream)
        msg = "source must implement the Arrow RbCapsule Interface (arrow_c_stream)"
        raise TypeError, msg
      end

      reader = RbArrowCStreamReader.new(source)

      io_source = lambda do |with_columns, predicate, n_rows, batch_size|
        remaining = n_rows
        while !(batch = reader.next_batch(with_columns)).nil?
          df = DataFrame._from_rbdf(batch)
          if !predicate.nil?
            df = df.filter(predicate)
          end
          if !remaining.nil?
            df = df.head(remaining)
            remaining -= df.height
          end
          yield df
          if !remaining.nil? && remaining <= 0
            return
          end
        end
      end

      Plugins.register_io_source(io_source, schema: reader.schema)
    end
  end
end
