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

    def initialize(restore_defaults: false)
      @original_state = self.class.save

      if restore_defaults
        self.class.restore_defaults
      end

      yield self.class

      self.class.restore_defaults.load(@original_state)
      @original_state = ""
    end

    def self.set_tbl_rows(n)
      ENV["POLARS_FMT_MAX_ROWS"] = n.to_s
    end

    def self.load(cfg)
      options = JSON.parse(cfg)
      ENV.merge!(options["environment"])
      self
    end

    def self.restore_defaults
      POLARS_CFG_ENV_VARS.each do |var|
        ENV.delete(var)
      end
      set_fmt_float
      self
    end

    def self.save
      environment_vars = POLARS_CFG_ENV_VARS.sort.select { |k| ENV.key?(k) }.to_h { |k| [k, ENV[k]] }
      direct_vars = {}
      options = JSON.generate({environment: environment_vars, direct: direct_vars})
      options
    end

    def self.set_fmt_float(fmt = "mixed")
      Polars._set_float_fmt(fmt)
    end
  end
end
