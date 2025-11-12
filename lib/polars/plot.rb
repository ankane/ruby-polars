module Polars
  module Plot
    # Plot data.
    #
    # @return [Object]
    def plot(x = nil, y = nil, type: nil, group: nil, stacked: nil)
      plot = DataFramePlot.new(self)
      return plot if x.nil? && y.nil?

      raise ArgumentError, "Must specify columns" if x.nil? || y.nil?
      type ||= begin
        if self[x].dtype.numeric? && self[y].dtype.numeric?
          "scatter"
        elsif self[x].dtype == String && self[y].dtype.numeric?
          "column"
        elsif (self[x].dtype == Date || self[x].dtype == Datetime) && self[y].dtype.numeric?
          "line"
        else
          raise "Cannot determine type. Use the type option."
        end
      end

      plot.send(:plot, x, y, type: type, group: group, stacked: stacked)
    end
  end
end
