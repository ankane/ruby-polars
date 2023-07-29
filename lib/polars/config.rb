module Polars
  # Configure polars; offers options for table formatting and more.
  class Config
    POLARS_CFG_ENV_VARS = [
      "POLARS_ACTIVATE_DECIMAL",
      "POLARS_AUTO_STRUCTIFY",
      "POLARS_FMT_MAX_COLS",
      "POLARS_FMT_MAX_ROWS",
      "POLARS_FMT_STR_LEN",
      "POLARS_FMT_TABLE_CELL_ALIGNMENT",
      "POLARS_FMT_TABLE_DATAFRAME_SHAPE_BELOW",
      "POLARS_FMT_TABLE_FORMATTING",
      "POLARS_FMT_TABLE_HIDE_COLUMN_DATA_TYPES",
      "POLARS_FMT_TABLE_HIDE_COLUMN_NAMES",
      "POLARS_FMT_TABLE_HIDE_COLUMN_SEPARATOR",
      "POLARS_FMT_TABLE_HIDE_DATAFRAME_SHAPE_INFORMATION",
      "POLARS_FMT_TABLE_INLINE_COLUMN_DATA_TYPE",
      "POLARS_FMT_TABLE_ROUNDED_CORNERS",
      "POLARS_STREAMING_CHUNK_SIZE",
      "POLARS_TABLE_WIDTH",
      "POLARS_VERBOSE"
    ]

    # Initialize a Config object instance for context manager usage.
    def initialize(restore_defaults: false, **options)
      @original_state = self.class.save

      if restore_defaults
        self.class.restore_defaults
      end

      options.each do |opt, value|
        opt = "set_#{opt}" unless opt.to_s.start_with?("set_")
        if !self.class.respond_to?(opt)
          raise ArgumentError, "Config has no #{opt} option"
        end
        self.class.public_send(opt, value)
      end

      yield self.class

      self.class.restore_defaults.load(@original_state)
      @original_state = ""
    end

    # Load and set previously saved (or shared) Config options from json/file.
    #
    # @return [Config]
    def self.load(cfg)
      options = JSON.parse(cfg)
      ENV.merge!(options["environment"])
      self
    end

    # Reset all polars Config settings to their default state.
    #
    # @return [Config]
    def self.restore_defaults
      POLARS_CFG_ENV_VARS.each do |var|
        ENV.delete(var)
      end
      set_fmt_float
      self
    end

    # Save the current set of Config options as a json string or file.
    #
    # @return [Config]
    def self.save
      environment_vars = POLARS_CFG_ENV_VARS.sort.select { |k| ENV.key?(k) }.to_h { |k| [k, ENV[k]] }
      direct_vars = {}
      options = JSON.generate({environment: environment_vars, direct: direct_vars})
      options
    end

    # Control how floating  point values are displayed.
    #
    # @return [Config]
    def self.set_fmt_float(fmt = "mixed")
      Polars._set_float_fmt(fmt)
      self
    end

    # Set the max number of rows used to draw the table (both Dataframe and Series).
    #
    # @param n [Integer]
    #   number of rows to display; if `n < 0` (eg: -1), display all
    #   rows (DataFrame) and all elements (Series).
    #
    # @return [Config]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"abc" => [1.0, 2.5, 3.5, 5.0], "xyz" => [true, false, true, false]}
    #   )
    #   Polars::Config.new(tbl_rows: 2) do
    #     p df
    #   end
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬───────┐
    #   # │ abc ┆ xyz   │
    #   # │ --- ┆ ---   │
    #   # │ f64 ┆ bool  │
    #   # ╞═════╪═══════╡
    #   # │ 1.0 ┆ true  │
    #   # │ …   ┆ …     │
    #   # │ 5.0 ┆ false │
    #   # └─────┴───────┘
    def self.set_tbl_rows(n)
      ENV["POLARS_FMT_MAX_ROWS"] = n.to_s
      self
    end

    # Set the number of characters used to draw the table.
    #
    # @param width [Integer]
    #   number of chars
    #
    # @return [Config]
    def self.set_tbl_width_chars(width)
      ENV["POLARS_TABLE_WIDTH"] = width.to_s
      self
    end

    # Enable additional verbose/debug logging.
    #
    # @return [Config]
    def self.set_verbose(active = true)
      ENV["POLARS_VERBOSE"] = active ? "1" : "0"
      self
    end
  end
end
