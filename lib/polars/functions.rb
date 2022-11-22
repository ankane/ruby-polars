module Polars
  module Functions
    def concat(items, rechunk: true, how: "vertical", parallel: true)
      if items.empty?
        raise ArgumentError, "cannot concat empty list"
      end

      first = items[0]
      if first.is_a?(DataFrame)
        if how == "vertical"
          out = Utils.wrap_df(_concat_df(items))
        elsif how == "diagonal"
          out = Utils.wrap_df(_diag_concat_df(items))
        elsif how == "horizontal"
          out = Utils.wrap_df(_hor_concat_df(items))
        else
          raise ArgumentError, "how must be one of {{'vertical', 'diagonal', 'horizontal'}}, got #{how}"
        end
      elsif first.is_a?(LazyFrame)
        if how == "vertical"
          # TODO
          return Utils.wrap_ldf(_concat_lf(items, rechunk, parallel))
        else
          raise ArgumentError, "Lazy only allows 'vertical' concat strategy."
        end
      elsif first.is_a?(Series)
        # TODO
        out = Utils.wrap_s(_concat_series(items))
      elsif first.is_a?(Expr)
        out = first
        items[1..-1].each do |e|
          out = out.append(e)
        end
      else
        raise ArgumentError, "did not expect type: #{first.class.name} in 'Polars.concat'."
      end

      if rechunk
        out.rechunk
      else
        out
      end
    end
  end
end
