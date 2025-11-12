module Polars
  # DataFrame.plot namespace.
  class DataFramePlot
    # @private
    def initialize(df)
      require "vega"

      @df = df
    end

    def line(x, y, color: nil, _type: "line")
      data = @df[[x, y].map(&:to_s).uniq].rows(named: true)

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

    def area(x, y, color: nil)
      line(x, y, color: color, _type: "area")
    end

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
