module Polars
  class LazyGroupBy
    def initialize(lgb, lazyframe_class)
      @lgb = lgb
      @lazyframe_class = lazyframe_class
    end

    def agg(aggs)
      rbexprs = Utils.selection_to_rbexpr_list(aggs)
      @lazyframe_class._from_rbldf(@lgb.agg(rbexprs))
    end
  end
end
