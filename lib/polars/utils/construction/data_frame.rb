module Polars
  module Utils
    def self.hash_to_rbdf(data, schema: nil, schema_overrides: nil, strict: true, nan_to_null: nil)
      if schema.is_a?(Hash) && !data.empty?
        if !data.all? { |col, _| schema[col] }
          raise ArgumentError, "The given column-schema names do not match the data hash"
        end

        data = schema.to_h { |col| [col, data[col]] }
      end

      column_names, schema_overrides = _unpack_schema(
        schema, lookup_names: data.keys, schema_overrides: schema_overrides
      )
      if column_names.empty?
        column_names = data.keys
      end

      if data.empty? && !schema_overrides.empty?
        data_series = column_names.map { |name| Series.new(name, [], dtype: schema_overrides[name], strict: strict, nan_to_null: nan_to_null)._s }
      else
        data_series = _expand_hash_values(data, schema_overrides: schema_overrides, strict: strict, nan_to_null: nan_to_null).values.map(&:_s)
      end

      data_series = _handle_columns_arg(data_series, columns: column_names, from_hash: true)
      RbDataFrame.new(data_series)
    end

    def self._unpack_schema(schema, schema_overrides: nil, n_expected: nil, lookup_names: nil, include_overrides_in_columns: false)
      if schema.is_a?(Hash)
        schema = schema.to_a
      end
      column_names =
        (schema || []).map.with_index do |col, i|
          if col.is_a?(::String)
            col || "column_#{i}"
          else
            col[0]
          end
        end
      if column_names.empty? && n_expected
        column_names = n_expected.times.map { |i| "column_#{i}" }
      end
      # TODO zip_longest
      lookup = column_names.zip(lookup_names || []).to_h

      column_dtypes =
        (schema || []).select { |col| !col.is_a?(::String) && col[1] }.to_h do |col|
          [lookup[col[0]] || col[0], col[1]]
        end

      if schema_overrides && schema_overrides.any?
        column_dtypes.merge!(schema_overrides)
      end

      column_dtypes.each do |col, dtype|
        if !Utils.is_polars_dtype(dtype, include_unknown: true) && !dtype.nil?
          column_dtypes[col] = Utils.rb_type_to_dtype(dtype)
        end
      end

      [column_names, column_dtypes]
    end

    def self._handle_columns_arg(data, columns: nil, from_hash: false)
      if columns.nil? || columns.empty?
        data
      else
        if data.empty?
          columns.map { |c| Series.new(c, nil)._s }
        elsif data.length == columns.length
          if from_hash
            series_map = data.to_h { |s| [s.name, s] }
            if columns.all? { |col| series_map.key?(col) }
              return columns.map { |col| series_map[col] }
            end
          end

          columns.each_with_index do |c, i|
            # not in-place?
            data[i].rename(c)
          end
          data
        else
          raise ArgumentError, "Dimensions of columns arg must match data dimensions."
        end
      end
    end

    def self._post_apply_columns(rbdf, columns, structs: nil, schema_overrides: nil, strict: true)
      rbdf_columns = rbdf.columns
      rbdf_dtypes = rbdf.dtypes
      columns, dtypes = _unpack_schema(
        (columns || rbdf_columns), schema_overrides: schema_overrides
      )
      column_subset = []
      if columns != rbdf_columns
        if columns.length < rbdf_columns.length && columns == rbdf_columns.first(columns.length)
          column_subset = columns
        else
          rbdf.set_column_names(columns)
        end
      end

      column_casts = []
      columns.each_with_index do |col, i|
        if dtypes[col] == Categorical # != rbdf_dtypes[i]
          column_casts << Polars.col(col).cast(Categorical, strict: strict)._rbexpr
        elsif structs&.any? && structs.include?(col) && structs[col] != rbdf_dtypes[i]
          column_casts << Polars.col(col).cast(structs[col], strict: strict)._rbexpr
        elsif dtypes.include?(col) && dtypes[col] != rbdf_dtypes[i]
          column_casts << Polars.col(col).cast(dtypes[col], strict: strict)._rbexpr
        end
      end

      if column_casts.any? || column_subset.any?
        rbdf = rbdf.lazy
        if column_casts.any?
          rbdf = rbdf.with_columns(column_casts)
        end
        if column_subset.any?
          rbdf = rbdf.select(column_subset.map { |col| Polars.col(col)._rbexpr })
        end
        rbdf = rbdf.collect
      end

      rbdf
    end

    def self._expand_hash_values(data, schema_overrides: nil, strict: true, order: nil, nan_to_null: false)
      updated_data = {}
      unless data.empty?
        dtypes = schema_overrides || {}
        array_len = data.values.map { |val| Utils.arrlen(val) || 0 }.max
        if array_len > 0
          data.each do |name, val|
            dtype = dtypes[name]
            if val.is_a?(Hash) && dtype != Struct
              updated_data[name] = DataFrame.new(val, strict: strict).to_struct(name)
            elsif !Utils.arrlen(val).nil?
              updated_data[name] = Series.new(::String.new(name), val, dtype: dtype, strict: strict)
            elsif val.nil? || [Integer, Float, TrueClass, FalseClass, ::String, ::Date, ::DateTime, ::Time].any? { |cls| val.is_a?(cls) }
              dtype = Polars::Float64 if val.nil? && dtype.nil?
              updated_data[name] = Series.new(::String.new(name), [val], dtype: dtype, strict: strict).extend_constant(val, array_len - 1)
            else
              raise Todo
            end
          end
        elsif data.values.all? { |val| Utils.arrlen(val) == 0 }
          data.each do |name, val|
            updated_data[name] = Series.new(name, val, dtype: dtypes[name], strict: strict)
          end
        elsif data.values.all? { |val| Utils.arrlen(val).nil? }
          data.each do |name, val|
            updated_data[name] = Series.new(name, [val], dtype: dtypes[name], strict: strict)
          end
        end
      end
      updated_data
    end

    def self.sequence_to_rbdf(
      data,
      schema: nil,
      schema_overrides: nil,
      strict: true,
      orient: nil,
      infer_schema_length: N_INFER_DEFAULT,
      nan_to_null: false
    )
      if data.empty?
        return hash_to_rbdf({}, schema: schema, schema_overrides: schema_overrides)
      end

      _sequence_to_rbdf_dispatcher(
        get_first_non_none(data),
        data,
        schema,
        schema_overrides: schema_overrides,
        strict: strict,
        orient: orient,
        infer_schema_length: infer_schema_length,
        nan_to_null: nan_to_null
      )
    end

    def self._sequence_to_rbdf_dispatcher(
      first_element,
      data,
      schema,
      schema_overrides: nil,
      strict: true,
      orient: nil,
      infer_schema_length: nil,
      nan_to_null: false
    )
      common_params = {
        data: data,
        schema: schema,
        schema_overrides: schema_overrides,
        strict: strict,
        orient: orient,
        infer_schema_length: infer_schema_length,
        nan_to_null: nan_to_null
      }

      if first_element.is_a?(Series)
        to_rbdf = method(:_sequence_of_series_to_rbdf)
      elsif first_element.is_a?(::Array)
        to_rbdf = method(:_sequence_of_sequence_to_rbdf)
      elsif first_element.is_a?(Hash)
        to_rbdf = method(:_sequence_of_dict_to_rbdf)
      else
        to_rbdf = method(:_sequence_of_elements_to_rbdf)
      end

      common_params[:first_element] = first_element
      to_rbdf.(**common_params)
    end

    def self._sequence_of_sequence_to_rbdf(
      first_element:,
      data:,
      schema:,
      schema_overrides:,
      strict:,
      orient:,
      infer_schema_length:,
      nan_to_null: false
    )
      if orient.nil?
        if schema.nil?
          orient = "col"
        else
          # Try to infer orientation from schema length and data dimensions
          is_row_oriented = schema.length == first_element.length && schema.length != data.length
          orient = is_row_oriented ? "row" : "col"

          if is_row_oriented
            Utils.issue_warning(
              "Row orientation inferred during DataFrame construction." +
              ' Explicitly specify the orientation by passing `orient: "row"` to silence this warning.'
            )
          end
        end
      end

      if orient == "row"
        column_names, schema_overrides = _unpack_schema(
          schema, schema_overrides: schema_overrides, n_expected: first_element.length
        )
        local_schema_override =
          if schema_overrides
            _include_unknowns(schema_overrides, column_names)
          else
            {}
          end

        unpack_nested = false
        local_schema_override.each do |col, tp|
          if [Categorical, Enum].include?(tp)
            local_schema_override[col] = String
          elsif !unpack_nested && [Unknown, Struct].include?(tp.base_type)
            # TODO fix
            unpack_nested = false
          end
        end

        if unpack_nested
          raise Todo
        else
          rbdf = RbDataFrame.from_rows(
            data,
            infer_schema_length,
            local_schema_override
          )
        end
        if column_names.any? || schema_overrides.any?
          rbdf = _post_apply_columns(
            rbdf, column_names, schema_overrides: schema_overrides, strict: strict
          )
        end
        rbdf

      elsif orient == "col"
        column_names, schema_overrides = _unpack_schema(
          schema, schema_overrides: schema_overrides, n_expected: data.length
        )
        data_series =
          data.map.with_index do |element, i|
            Series.new(
              column_names[i],
              element,
              dtype: schema_overrides[column_names[i]],
              strict: strict,
              nan_to_null: nan_to_null
            )._s
          end
        RbDataFrame.new(data_series)

      else
        msg = "`orient` must be one of {{'col', 'row', None}}, got #{orient.inspect}"
        raise ArgumentError, msg
      end
    end

    def self._sequence_of_series_to_rbdf(
      first_element:,
      data:,
      schema:,
      schema_overrides:,
      strict:,
      **kwargs
    )
      series_names = data.map { |s| s.name }
      column_names, schema_overrides = _unpack_schema(
        schema || series_names,
        schema_overrides: schema_overrides,
        n_expected: data.length
      )
      data_series = []
      data.each_with_index do |s, i|
        if !s.name
          s = s.alias(column_names[i])
        end
        new_dtype = schema_overrides[column_names[i]]
        if new_dtype && new_dtype != s.dtype
          s = s.cast(new_dtype, strict: strict, wrap_numerical: false)
        end
        data_series << s._s
      end

      data_series = _handle_columns_arg(data_series, columns: column_names)
      RbDataFrame.new(data_series)
    end

    def self._sequence_of_dict_to_rbdf(
      first_element:,
      data:,
      schema:,
      schema_overrides:,
      strict:,
      infer_schema_length:,
      **kwargs
    )
      column_names, schema_overrides = _unpack_schema(
        schema, schema_overrides: schema_overrides
      )
      dicts_schema =
        if column_names.any?
          _include_unknowns(schema_overrides, column_names || schema_overrides.to_a)
        else
          nil
        end

      rbdf = RbDataFrame.from_hashes(
        data,
        dicts_schema,
        schema_overrides,
        strict,
        infer_schema_length
      )
      rbdf
    end

    def self._sequence_of_elements_to_rbdf(
      first_element:,
      data:,
      schema:,
      schema_overrides:,
      strict:,
      **kwargs
    )
      column_names, schema_overrides = _unpack_schema(
        schema, schema_overrides: schema_overrides, n_expected: 1
      )
      data_series = [
        Series.new(
          column_names[0],
          data,
          dtype: schema_overrides[column_names[0]],
          strict: strict
        )._s
      ]
      data_series = _handle_columns_arg(data_series, columns: column_names)
      RbDataFrame.new(data_series)
    end

    def self.get_first_non_none(values)
      if !values.nil?
        values.find { |v| !v.nil? }
      end
    end

    def self._include_unknowns(schema, cols)
      cols.to_h { |col| [col, schema[col] || Unknown] }
    end

    def self.series_to_rbdf(data, schema: nil, schema_overrides: nil, strict: true)
      data_series = [data._s]
      series_name = data_series.map(&:name)
      column_names, schema_overrides = _unpack_schema(
        schema || series_name, schema_overrides: schema_overrides, n_expected: 1
      )
      if schema_overrides.any?
        new_dtype = schema_overrides.values[0]
        if new_dtype != data.dtype
          data_series[0] = data_series[0].cast(new_dtype, strict)
        end
      end

      data_series = _handle_columns_arg(data_series, columns: column_names)
      RbDataFrame.new(data_series)
    end
  end
end
