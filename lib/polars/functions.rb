module Polars
  module Functions
    # Convert categorical variables into dummy/indicator variables.
    #
    # @param df [DataFrame]
    #   DataFrame to convert.
    # @param columns [Array, nil]
    #   A subset of columns to convert to dummy variables. `nil` means
    #   "all columns".
    #
    # @return [DataFrame]
    def get_dummies(df, columns: nil)
      df.to_dummies(columns: columns)
    end

    # Aggregate to list.
    #
    # @return [Expr]
    def to_list(name)
      col(name).list
    end

    # Compute the spearman rank correlation between two columns.
    #
    # Missing data will be excluded from the computation.
    #
    # @param a [Object]
    #   Column name or Expression.
    # @param b [Object]
    #   Column name or Expression.
    # @param ddof [Integer]
    #   Delta degrees of freedom
    # @param propagate_nans [Boolean]
    #   If `True` any `NaN` encountered will lead to `NaN` in the output.
    #   Defaults to `False` where `NaN` are regarded as larger than any finite number
    #   and thus lead to the highest rank.
    #
    # @return [Expr]
    def spearman_rank_corr(a, b, ddof: 1, propagate_nans: false)
      corr(a, b, method: "spearman", ddof: ddof, propagate_nans: propagate_nans)
    end

    # Compute the pearson's correlation between two columns.
    #
    # @param a [Object]
    #   Column name or Expression.
    # @param b [Object]
    #   Column name or Expression.
    # @param ddof [Integer]
    #   Delta degrees of freedom
    #
    # @return [Expr]
    def pearson_corr(a, b, ddof: 1)
      corr(a, b, method: "pearson", ddof: ddof)
    end
  end
end
