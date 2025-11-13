module Polars
  # Series.plot namespace.
  class SeriesPlot
    # @private
    def initialize(s)
      require "vega"

      name = s.name || "value"
      @df = s.to_frame(name)
      @series_name = name
    end

    # Draw histogram.
    #
    # @return [Vega::LiteChart]
    def hist
      encoding = {
        x: {field: @series_name, bin: true},
        y: {aggregate: "count"}
      }

      Vega.lite
        .data(@df.rows(named: true))
        .mark(type: "bar", tooltip: true)
        .encoding(encoding)
        .config(axis: {labelFontSize: 12})
    end

    # Draw kernel density estimate plot.
    #
    # @return [Vega::LiteChart]
    def kde
      if @series_name == "density"
        msg = "cannot use `plot.kde` when Series name is `'density'`"
        raise ArgumentError, msg
      end

      encoding = {
        x: {field: @series_name, type: "quantitative"},
        y: {field: "density", type: "quantitative"}
      }

      Vega.lite
        .data(@df.rows(named: true))
        .transform(density: @series_name, as: [@series_name, "density"])
        .mark(type: "area", tooltip: true)
        .encoding(encoding)
        .config(axis: {labelFontSize: 12})
    end

    # Draw line plot.
    #
    # @return [Vega::LiteChart]
    def line
      if @series_name == "index"
        msg = "cannot call `plot.line` when Series name is 'index'"
        raise ArgumentError, msg
      end

      encoding = {
        x: {field: "index", type: "quantitative"},
        y: {field: @series_name, type: "quantitative"}
      }

      Vega.lite
        .data(@df.with_row_index.rows(named: true))
        .mark(type: "line", tooltip: true)
        .encoding(encoding)
        .config(axis: {labelFontSize: 12})
    end
  end
end
