module Polars
  # DataFrame.plot namespace.
  class DataFramePlot
    # @private
    def initialize(df)
      require "vega"

      @df = df
    end

    # Draw line plot.
    #
    # @param x [String]
    #   Column with x-coordinates of lines.
    # @param y [String]
    #   Column with y-coordinates of lines.
    # @param color [String]
    #   Column to color lines by.
    #
    # @return [Vega::LiteChart]
    def line(x, y, color: nil, _type: "line")
      data = @df[[x, y, color].compact.map(&:to_s).uniq].rows(named: true)

      x_type =
        if @df[x].dtype.numeric?
          "quantitative"
        elsif @df[x].dtype.temporal?
          "temporal"
        else
          "nominal"
        end

      scale = x_type == "temporal" ? {type: "utc"} : {}
      encoding = {
        x: {field: x, type: x_type, scale: scale},
        y: {field: y, type: "quantitative"}
      }
      encoding[:color] = {field: color} if color

      Vega.lite
        .data(data)
        .mark(type: _type, tooltip: true, interpolate: "cardinal", point: {size: 60})
        .encoding(encoding)
        .config(axis: {labelFontSize: 12})
    end

    # Draw area plot.
    #
    # @param x [String]
    #   Column with x-coordinates of lines.
    # @param y [String]
    #   Column with y-coordinates of lines.
    # @param color [String]
    #   Column to color lines by.
    #
    # @return [Vega::LiteChart]
    def area(x, y, color: nil)
      line(x, y, color: color, _type: "area")
    end

    # Draw pie chart.
    #
    # @param x [String]
    #   Column with label of slice.
    # @param y [String]
    #   Column with size of slice.
    #
    # @return [Vega::LiteChart]
    def pie(x, y)
      data = @df[[x, y].map(&:to_s).uniq].rows(named: true)

      Vega.lite
        .data(data)
        .mark(type: "arc", tooltip: true)
        .encoding(
          color: {field: x, type: "nominal", sort: "none", axis: {title: nil}, legend: {labelFontSize: 12}},
          theta: {field: y, type: "quantitative"}
        )
        .view(stroke: nil)
    end

    # Draw column plot.
    #
    # @param x [String]
    #   Column with x-coordinates of columns.
    # @param y [String]
    #   Column with y-coordinates of columns.
    # @param color [String]
    #   Column to color columns by.
    # @param stacked [Boolean]
    #   Stack columns.
    #
    # @return [Vega::LiteChart]
    def column(x, y, color: nil, stacked: nil)
      data = @df[[x, y, color].compact.map(&:to_s).uniq].rows(named: true)

      encoding = {
        x: {field: x, type: "nominal", sort: "none", axis: {labelAngle: 0}},
        y: {field: y, type: "quantitative"}
      }
      if color
        encoding[:color] = {field: color}
        encoding[:xOffset] = {field: color} unless stacked
      end

      Vega.lite
        .data(data)
        .mark(type: "bar", tooltip: true)
        .encoding(encoding)
        .config(axis: {labelFontSize: 12})
    end

    # Draw bar plot.
    #
    # @param x [String]
    #   Column with x-coordinates of bars.
    # @param y [String]
    #   Column with y-coordinates of bars.
    # @param color [String]
    #   Column to color bars by.
    # @param stacked [Boolean]
    #   Stack bars.
    #
    # @return [Vega::LiteChart]
    def bar(x, y, color: nil, stacked: nil)
      data = @df[[x, y, color].compact.map(&:to_s).uniq].rows(named: true)

      encoding = {
        # TODO determine label angle
        y: {field: x, type: "nominal", sort: "none", axis: {labelAngle: 0}},
        x: {field: y, type: "quantitative"}
      }
      if color
        encoding[:color] = {field: color}
        encoding[:yOffset] = {field: color} unless stacked
      end

      Vega.lite
        .data(data)
        .mark(type: "bar", tooltip: true)
        .encoding(encoding)
        .config(axis: {labelFontSize: 12})
    end

    # Draw scatter plot.
    #
    # @param x [String]
    #   Column with x-coordinates of points.
    # @param y [String]
    #   Column with y-coordinates of points.
    # @param color [String]
    #   Column to color points by.
    #
    # @return [Vega::LiteChart]
    def scatter(x, y, color: nil)
      data = @df[[x, y, color].compact.map(&:to_s).uniq].rows(named: true)

      encoding = {
        x: {field: x, type: "quantitative", scale: {zero: false}},
        y: {field: y, type: "quantitative", scale: {zero: false}},
        size: {value: 60}
      }
      encoding[:color] = {field: color} if color

      Vega.lite
        .data(data)
        .mark(type: "circle", tooltip: true)
        .encoding(encoding)
        .config(axis: {labelFontSize: 12})
    end
    alias_method :point, :scatter
  end
end
