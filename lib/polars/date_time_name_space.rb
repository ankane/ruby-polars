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
  end
end
