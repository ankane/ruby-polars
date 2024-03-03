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
  end
end
