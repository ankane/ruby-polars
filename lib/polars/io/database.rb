module Polars
  module IO
    # Read a SQL query into a DataFrame.
    #
    # @param query [Object]
    #   ActiveRecord::Relation or ActiveRecord::Result.
    # @param schema_overrides [Hash]
    #   A hash mapping column names to dtypes, used to override the schema
    #   inferred from the query.
    #
    # @return [DataFrame]
    def read_database(query, schema_overrides: nil)
      if !defined?(ActiveRecord)
        raise Error, "Active Record not available"
      end

      result =
        if query.is_a?(ActiveRecord::Result)
          query
        elsif query.is_a?(ActiveRecord::Relation)
          query.connection_pool.with_connection { |c| c.select_all(query.to_sql) }
        elsif query.is_a?(::String)
          ActiveRecord::Base.connection_pool.with_connection { |c| c.select_all(query) }
        else
          raise ArgumentError, "Expected ActiveRecord::Relation, ActiveRecord::Result, or String"
        end

      data = {}
      schema_overrides = (schema_overrides || {}).transform_keys(&:to_s)

      result.columns.each_with_index do |k, i|
        column_type = result.column_types[i]

        data[k] =
          if column_type
            result.rows.map { |r| column_type.deserialize(r[i]) }
          else
            result.rows.map { |r| r[i] }
          end

        polars_type =
          case column_type&.type
          when :binary
            Binary
          when :boolean
            Boolean
          when :date
            Date
          when :datetime, :timestamp
            Datetime
          when :decimal
            Decimal
          when :float
            Float64
          when :integer
            Int64
          when :string, :text
            String
          when :time
            Time
          # TODO fix issue with null
          # when :json, :jsonb
          #   Struct
          end

        schema_overrides[k] ||= polars_type if polars_type
      end

      DataFrame.new(data, schema_overrides: schema_overrides)
    end
    alias_method :read_sql, :read_database
  end
end
