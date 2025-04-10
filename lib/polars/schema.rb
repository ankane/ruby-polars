module Polars
  class Schema
    def initialize(schema, check_dtypes: true)
      raise Todo if check_dtypes
      @schema = schema.to_h
    end

    def [](key)
      @schema[key]
    end

    def names
      @schema.keys
    end

    def dtypes
      @schema.values
    end

    def length
      @schema.length
    end

    def to_s
      "#{self.class.name}(#{@schema})"
    end
    alias_method :inspect, :to_s
  end
end
