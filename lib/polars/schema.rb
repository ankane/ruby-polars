module Polars
  class Schema
    # Ordered mapping of column names to their data type.
    #
    # @param schema [Object]
    #   The schema definition given by column names and their associated
    #   Polars data type. Accepts a mapping or an enumerable of arrays.
    def initialize(schema = nil, check_dtypes: true)
      input = schema || {}
      @schema = {}
      input.each do |name, tp|
        if !check_dtypes
          @schema[name] = tp
        elsif Utils.is_polars_dtype(tp)
          @schema[name] = _check_dtype(tp)
        else
          self[name] = tp
        end
      end
    end

    # Returns the data type of the column.
    #
    # @return [Object]
    def [](key)
      @schema[key]
    end

    # Sets the data type of the column.
    #
    # @return [Object]
    def []=(name, dtype)
      _check_dtype(dtype)
      @schema[name] = dtype
    end

    # Get the column names of the schema.
    #
    # @return [Array]
    #
    # @example
    #   s = Polars::Schema.new({"x" => Polars::Float64.new, "y" => Polars::Datetime.new(time_zone: "UTC")})
    #   s.names
    #   # => ["x", "y"]
    def names
      @schema.keys
    end

    # Get the data types of the schema.
    #
    # @return [Array]
    #
    # @example
    #   s = Polars::Schema.new({"x" => Polars::UInt8.new, "y" => Polars::List.new(Polars::UInt8)})
    #   s.dtypes
    #   # => [Polars::UInt8, Polars::List(Polars::UInt8)]
    def dtypes
      @schema.values
    end

    # Get the number of schema entries.
    #
    # @return [Integer]
    #
    # @example
    #   s = Polars::Schema.new({"x" => Polars::Int32.new, "y" => Polars::List.new(Polars::String)})
    #   s.length
    #   # => 2
    def length
      @schema.length
    end

    # Returns a string representing the Schema.
    #
    # @return [String]
    def to_s
      "#{self.class.name}(#{@schema})"
    end
    alias_method :inspect, :to_s

    # @private
    def include?(name)
      @schema.include?(name)
    end

    # @private
    def to_h
      @schema.to_h
    end

    private

    def _check_dtype(tp)
      if !tp.is_a?(DataType)
        # note: if nested/decimal, or has signature params, this implies required args
        if tp.nested? || tp.decimal? || _required_init_args(tp)
          msg = "dtypes must be fully-specified, got: #{tp.inspect}"
          raise TypeError, msg
        end
        tp = tp.new
      end
      tp
    end

    def _required_init_args(tp)
      arity = tp.method(:new).arity
      arity > 0 || arity < -1
    end
  end
end
