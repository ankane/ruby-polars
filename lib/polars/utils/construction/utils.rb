module Polars
  module Utils
    def self.get_first_non_none(values)
      if !values.nil?
        values.find { |v| !v.nil? }
      end
    end
  end
end
