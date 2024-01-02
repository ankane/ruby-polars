module Polars
  # Run SQL queries against DataFrame/LazyFrame data.
  class SQLContext
    # @private
    attr_accessor :_ctxt, :_eager_execution

    # Initialize a new `SQLContext`.
    def initialize(frames = nil, eager_execution: false, **named_frames)
      self._ctxt = RbSQLContext.new
      self._eager_execution = eager_execution

      frames = (frames || {}).to_h

      if frames.any? || named_frames.any?
        register_many(frames, **named_frames)
      end
    end

    # Parse the given SQL query and execute it against the registered frame data.
    #
    # @param query [String]
    #   A valid string SQL query.
    # @param eager [Boolean]
    #   Apply the query eagerly, returning `DataFrame` instead of `LazyFrame`.
    #   If unset, the value of the init-time parameter "eager_execution" will be
    #   used. (Note that the query itself is always executed in lazy-mode; this
    #   parameter only impacts the type of the returned frame).
    #
    # @return [Object]
    #
    # @example Execute a SQL query against the registered frame data:
    #   df = Polars::DataFrame.new(
    #     [
    #       ["The Godfather", 1972, 6_000_000, 134_821_952, 9.2],
    #       ["The Dark Knight", 2008, 185_000_000, 533_316_061, 9.0],
    #       ["Schindler's List", 1993, 22_000_000, 96_067_179, 8.9],
    #       ["Pulp Fiction", 1994, 8_000_000, 107_930_000, 8.9],
    #       ["The Shawshank Redemption", 1994, 25_000_000, 28_341_469, 9.3],
    #     ],
    #     schema: ["title", "release_year", "budget", "gross", "imdb_score"]
    #   )
    #   ctx = Polars::SQLContext.new(films: df)
    #   ctx.execute(
    #     "
    #     SELECT title, release_year, imdb_score
    #     FROM films
    #     WHERE release_year > 1990
    #     ORDER BY imdb_score DESC
    #     ",
    #     eager: true
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌──────────────────────────┬──────────────┬────────────┐
    #   # │ title                    ┆ release_year ┆ imdb_score │
    #   # │ ---                      ┆ ---          ┆ ---        │
    #   # │ str                      ┆ i64          ┆ f64        │
    #   # ╞══════════════════════════╪══════════════╪════════════╡
    #   # │ The Shawshank Redemption ┆ 1994         ┆ 9.3        │
    #   # │ The Dark Knight          ┆ 2008         ┆ 9.0        │
    #   # │ Schindler's List         ┆ 1993         ┆ 8.9        │
    #   # │ Pulp Fiction             ┆ 1994         ┆ 8.9        │
    #   # └──────────────────────────┴──────────────┴────────────┘
    #
    # @example Execute a GROUP BY query:
    #   ctx.execute(
    #     "
    #     SELECT
    #         MAX(release_year / 10) * 10 AS decade,
    #         SUM(gross) AS total_gross,
    #         COUNT(title) AS n_films,
    #     FROM films
    #     GROUP BY (release_year / 10) -- decade
    #     ORDER BY total_gross DESC
    #     ",
    #     eager: true
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌────────┬─────────────┬─────────┐
    #   # │ decade ┆ total_gross ┆ n_films │
    #   # │ ---    ┆ ---         ┆ ---     │
    #   # │ i64    ┆ i64         ┆ u32     │
    #   # ╞════════╪═════════════╪═════════╡
    #   # │ 2000   ┆ 533316061   ┆ 1       │
    #   # │ 1990   ┆ 232338648   ┆ 3       │
    #   # │ 1970   ┆ 134821952   ┆ 1       │
    #   # └────────┴─────────────┴─────────┘
    def execute(query, eager: nil)
      res = Utils.wrap_ldf(_ctxt.execute(query))
      eager || _eager_execution ? res.collect : res
    end

    # Register a single frame as a table, using the given name.
    #
    # @param name [String]
    #   Name of the table.
    # @param frame [Object]
    #   eager/lazy frame to associate with this table name.
    #
    # @return [SQLContext]
    #
    # @example
    #   df = Polars::DataFrame.new({"hello" => ["world"]})
    #   ctx = Polars::SQLContext.new
    #   ctx.register("frame_data", df).execute("SELECT * FROM frame_data").collect
    #   # =>
    #   # shape: (1, 1)
    #   # ┌───────┐
    #   # │ hello │
    #   # │ ---   │
    #   # │ str   │
    #   # ╞═══════╡
    #   # │ world │
    #   # └───────┘
    def register(name, frame)
      if frame.is_a?(DataFrame)
        frame = frame.lazy
      end
      _ctxt.register(name.to_s, frame._ldf)
      self
    end

    # Register multiple eager/lazy frames as tables, using the associated names.
    #
    # @param frames [Hash]
    #   A `{name:frame, ...}` mapping.
    # @param named_frames [Object]
    #   Named eager/lazy frames, provided as kwargs.
    #
    # @return [SQLContext]
    def register_many(frames, **named_frames)
      frames = (frames || {}).to_h
      frames = frames.merge(named_frames)
      frames.each do |name, frame|
        register(name, frame)
      end
      self
    end

    # Unregister one or more eager/lazy frames by name.
    #
    # @param names [Object]
    #   Names of the tables to unregister.
    #
    # @return [SQLContext]
    #
    # @example Register with a SQLContext object:
    #   df0 = Polars::DataFrame.new({"ints" => [9, 8, 7, 6, 5]})
    #   lf1 = Polars::LazyFrame.new({"text" => ["a", "b", "c"]})
    #   lf2 = Polars::LazyFrame.new({"misc" => ["testing1234"]})
    #   ctx = Polars::SQLContext.new(test1: df0, test2: lf1, test3: lf2)
    #   ctx.tables
    #   # => ["test1", "test2", "test3"]
    #
    # @example Unregister one or more of the tables:
    #   ctx.unregister(["test1", "test3"]).tables
    #   # => ["test2"]
    def unregister(names)
      if names.is_a?(::String)
        names = [names]
      end
      names.each do |nm|
        _ctxt.unregister(nm)
      end
      self
    end

    # Return a list of the registered table names.
    #
    # @return [Array]
    #
    # @example Executing as SQL:
    #   frame_data = Polars::DataFrame.new({"hello" => ["world"]})
    #   ctx = Polars::SQLContext.new(hello_world: frame_data)
    #   ctx.execute("SHOW TABLES", eager: true)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────────────┐
    #   # │ name        │
    #   # │ ---         │
    #   # │ str         │
    #   # ╞═════════════╡
    #   # │ hello_world │
    #   # └─────────────┘
    #
    # @example Calling the method:
    #   ctx.tables
    #   # => ["hello_world"]
    def tables
      _ctxt.get_tables.sort
    end
  end
end
