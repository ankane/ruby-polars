module Polars
  # Series.dt namespace.
  class DateTimeNameSpace
    include ExprDispatch

    self._accessor = "dt"

    # @private
    def initialize(series)
      self._s = series._s
    end

    # def [](item)
    # end

    # Return minimum as Ruby object.
    #
    # @return [Object]
    #
    # @example
    #   date = Polars.date_range(DateTime.new(2001, 1, 1), DateTime.new(2001, 1, 3), "1d")
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-02 00:00:00
    #   #         2001-01-03 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.min
    #   # => 2001-01-01 00:00:00 UTC
    def min
      Utils.wrap_s(_s).min
    end

    # Return maximum as Ruby object.
    #
    # @return [Object]
    #
    # @example
    #   date = Polars.date_range(DateTime.new(2001, 1, 1), DateTime.new(2001, 1, 3), "1d")
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-02 00:00:00
    #   #         2001-01-03 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.max
    #   # => 2001-01-03 00:00:00 UTC
    def max
      Utils.wrap_s(_s).max
    end

    # Return median as Ruby object.
    #
    # @return [Object]
    #
    # @example
    #   date = Polars.date_range(DateTime.new(2001, 1, 1), DateTime.new(2001, 1, 3), "1d")
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-02 00:00:00
    #   #         2001-01-03 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.median
    #   # => 2001-01-02 00:00:00 UTC
    def median
      s = Utils.wrap_s(_s)
      out = s.median.to_i
      Utils._to_ruby_datetime(out, s.dtype, tu: s.time_unit)
    end

    # Return mean as Ruby object.
    #
    # @return [Object]
    #
    # @example
    #   date = Polars.date_range(DateTime.new(2001, 1, 1), DateTime.new(2001, 1, 3), "1d")
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-02 00:00:00
    #   #         2001-01-03 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.mean
    #   # => 2001-01-02 00:00:00 UTC
    def mean
      s = Utils.wrap_s(_s)
      out = s.mean.to_i
      Utils._to_ruby_datetime(out, s.dtype, tu: s.time_unit)
    end

    # Format Date/datetime with a formatting rule.
    #
    # See [chrono strftime/strptime](https://docs.rs/chrono/0.4.19/chrono/format/strftime/index.html).
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 4)
    #   date = Polars.date_range(start, stop, "1d")
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-02 00:00:00
    #   #         2001-01-03 00:00:00
    #   #         2001-01-04 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.strftime("%Y-%m-%d")
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [str]
    #   # [
    #   #         "2001-01-01"
    #   #         "2001-01-02"
    #   #         "2001-01-03"
    #   #         "2001-01-04"
    #   # ]
    def strftime(fmt)
      super
    end

    # Extract the year from the underlying date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the year number in the calendar date.
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2002, 1, 1)
    #   date = Polars.date_range(start, stop, "1y")
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2002-01-01 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.year
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [i32]
    #   # [
    #   #         2001
    #   #         2002
    #   # ]
    def year
      super
    end

    # Extract ISO year from underlying Date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the year number according to the ISO standard.
    # This may not correspond with the calendar year.
    #
    # @return [Series]
    #
    # @example
    #   dt = DateTime.new(2022, 1, 1, 7, 8, 40)
    #   Polars::Series.new([dt]).dt.iso_year
    #   # =>
    #   # shape: (1,)
    #   # Series: '' [i32]
    #   # [
    #   #         2021
    #   # ]
    def iso_year
      super
    end
  end
end
