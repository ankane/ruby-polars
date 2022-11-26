module Polars
  class GroupBy
    attr_accessor :_df, :_dataframe_class, :by, :maintain_order

    def initialize(df, by, dataframe_class, maintain_order: false)
      self._df = df
      self._dataframe_class = dataframe_class
      self.by = by
      self.maintain_order = maintain_order
    end

    def agg(aggs)
      df = Utils.wrap_df(_df)
        .lazy
        .groupby(by, maintain_order: maintain_order)
        .agg(aggs)
        .collect(no_optimization: true, string_cache: false)
      _dataframe_class._from_rbdf(df._df)
    end
  end
end
