module Polars
  # Series.plot namespace.
  class SeriesPlot
    # @private
    attr_accessor :_df, :_series_name

    # @private
    def initialize(s)
      name = s.name || "value"
      self._df = s.to_frame(name)
      self._series_name = name
    end

    # Draw histogram.
    #
    # @return [Vega::LiteChart]
    def hist(**kwargs)
      raise Todo
    end
  end
end
