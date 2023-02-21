module Polars
  module Plot
    # Plot data.
    #
    # @return [Vega::LiteChart]
    def plot(x = nil, y = nil, type: nil, group: nil, stacked: nil)
      require "vega"

      raise ArgumentError, "Must specify columns" if columns.size != 2 && (!x || !y)
      x ||= columns[0]
      y ||= columns[1]
      type ||= begin
        if self[x].numeric? && self[y].numeric?
          "scatter"
        elsif self[x].utf8? && self[y].numeric?
          "column"
        elsif self[x].datelike? && self[y].numeric?
          "line"
        else
          raise "Cannot determine type. Use the type option."
        end
      end
      df = self[(group.nil? ? [x, y] : [x, y, group]).map(&:to_s).uniq]
      data = df.rows(named: true)

      case type
      when "line", "area"
        x_type =
          if df[x].numeric?
            "quantitative"
          elsif df[x].datelike?
            "temporal"
          else
            "nominal"
          end

        scale = x_type == "temporal" ? {type: "utc"} : {}
        encoding = {
          x: {field: x, type: x_type, scale: scale},
          y: {field: y, type: "quantitative"}
        }
        encoding[:color] = {field: group} if group

        Vega.lite
          .data(data)
          .mark(type: type, tooltip: true, interpolate: "cardinal", point: {size: 60})
          .encoding(encoding)
          .config(axis: {labelFontSize: 12})
      when "pie"
        raise ArgumentError, "Cannot use group option with pie chart" unless group.nil?

        Vega.lite
          .data(data)
          .mark(type: "arc", tooltip: true)
          .encoding(
            color: {field: x, type: "nominal", sort: "none", axis: {title: nil}, legend: {labelFontSize: 12}},
            theta: {field: y, type: "quantitative"}
          )
          .view(stroke: nil)
      when "column"
        encoding = {
          x: {field: x, type: "nominal", sort: "none", axis: {labelAngle: 0}},
          y: {field: y, type: "quantitative"}
        }
        if group
          encoding[:color] = {field: group}
          encoding[:xOffset] = {field: group} unless stacked
        end

        Vega.lite
          .data(data)
          .mark(type: "bar", tooltip: true)
          .encoding(encoding)
          .config(axis: {labelFontSize: 12})
      when "bar"
        encoding = {
          # TODO determine label angle
          y: {field: x, type: "nominal", sort: "none", axis: {labelAngle: 0}},
          x: {field: y, type: "quantitative"}
        }
        if group
          encoding[:color] = {field: group}
          encoding[:yOffset] = {field: group} unless stacked
        end

        Vega.lite
          .data(data)
          .mark(type: "bar", tooltip: true)
          .encoding(encoding)
          .config(axis: {labelFontSize: 12})
      when "scatter"
        encoding = {
          x: {field: x, type: "quantitative", scale: {zero: false}},
          y: {field: y, type: "quantitative", scale: {zero: false}},
          size: {value: 60}
        }
        encoding[:color] = {field: group} if group

        Vega.lite
          .data(data)
          .mark(type: "circle", tooltip: true)
          .encoding(encoding)
          .config(axis: {labelFontSize: 12})
      else
        raise ArgumentError, "Invalid type: #{type}"
      end
    end
  end
end
