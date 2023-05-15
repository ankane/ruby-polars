module Polars
  # @private
  module Utils
    DTYPE_TEMPORAL_UNITS = ["ns", "us", "ms"]

    def self.wrap_s(s)
      Series._from_rbseries(s)
    end

    def self.wrap_df(df)
      DataFrame._from_rbdf(df)
    end

    def self.wrap_ldf(ldf)
      LazyFrame._from_rbldf(ldf)
    end

    def self.wrap_expr(rbexpr)
      Expr._from_rbexpr(rbexpr)
    end

    def self.col(name)
      Polars.col(name)
    end

    def self.arrlen(obj)
      if obj.is_a?(Range)
        # size only works for numeric ranges
        obj.to_a.length
      elsif obj.is_a?(String)
        nil
      else
        obj.length
      end
    rescue
      nil
    end

    def self._timedelta_to_pl_duration(td)
      td
    end

    def self._datetime_to_pl_timestamp(dt, tu)
      if tu == "ns"
        (dt.to_datetime.to_time.to_f * 1e9).to_i
      elsif tu == "us"
        (dt.to_datetime.to_time.to_f * 1e6).to_i
      elsif tu == "ms"
        (dt.to_datetime.to_time.to_f * 1e3).to_i
      elsif tu.nil?
        (dt.to_datetime.to_time.to_f * 1e6).to_i
      else
        raise ArgumentError, "tu must be one of {{'ns', 'us', 'ms'}}, got #{tu}"
      end
    end

    def self._date_to_pl_date(d)
      dt = d.to_datetime.to_time
      dt.to_i / (3600 * 24)
    end

    def self._to_ruby_datetime(value, dtype, tu: "ns", tz: nil)
      if dtype == :date || dtype == Date
        # days to seconds
        # important to create from utc. Not doing this leads
        # to inconsistencies dependent on the timezone you are in.
        ::Time.at(value * 86400).utc.to_date
      # TODO fix dtype
      elsif dtype.to_s.start_with?("datetime[") || dtype.is_a?(Datetime)
        if tz.nil? || tz == ""
          if tu == "ns"
            raise Todo
          elsif tu == "us"
            dt = ::Time.at(value / 1000000, value % 1000000, :usec).utc
          elsif tu == "ms"
            raise Todo
          else
            raise ArgumentError, "tu must be one of {{'ns', 'us', 'ms'}}, got #{tu}"
          end
        else
          raise Todo
        end

        dt
      else
        raise NotImplementedError
      end
    end

    def self._to_ruby_duration(value, tu = "ns")
      if tu == "ns"
        value / 1e9
      elsif tu == "us"
        value / 1e6
      elsif tu == "ms"
        value / 1e3
      else
        raise ArgumentError, "tu must be one of {{'ns', 'us', 'ms'}}, got #{tu}"
      end
    end

    def self.selection_to_rbexpr_list(exprs)
      if exprs.is_a?(String) || exprs.is_a?(Symbol) || exprs.is_a?(Expr) || exprs.is_a?(Series)
        exprs = [exprs]
      end

      exprs.map { |e| expr_to_lit_or_expr(e, str_to_lit: false)._rbexpr }
    end

    def self.expr_to_lit_or_expr(expr, str_to_lit: true)
      if (expr.is_a?(String) || expr.is_a?(Symbol)) && !str_to_lit
        col(expr)
      elsif expr.is_a?(Integer) || expr.is_a?(Float) || expr.is_a?(String) || expr.is_a?(Symbol) || expr.is_a?(Series) || expr.nil?
        lit(expr)
      elsif expr.is_a?(Expr)
        expr
      else
        raise ArgumentError, "did not expect value #{expr} of type #{expr.class.name}, maybe disambiguate with Polars.lit or Polars.col"
      end
    end

    def self.lit(value)
      Polars.lit(value)
    end

    def self.normalise_filepath(path, check_not_directory: true)
      path = File.expand_path(path)
      if check_not_directory && File.exist?(path) && Dir.exist?(path)
        raise ArgumentError, "Expected a file path; #{path} is a directory"
      end
      path
    end

    # TODO fix
    def self.is_polars_dtype(data_type, include_unknown: false)
      if data_type == Unknown
        return include_unknown
      end
      data_type.is_a?(Symbol) || data_type.is_a?(String) || data_type.is_a?(DataType) || (data_type.is_a?(Class) && data_type < DataType)
    end

    RB_TYPE_TO_DTYPE = {
      Float => :f64,
      Integer => :i64,
      String => :str,
      TrueClass => :bool,
      FalseClass => :bool,
      ::Date => :date,
      ::DateTime => :datetime,
      ::Time => :datetime
    }

    # TODO fix
    def self.rb_type_to_dtype(data_type)
      if is_polars_dtype(data_type)
        data_type = data_type.to_s if data_type.is_a?(Symbol)
        return data_type
      end

      begin
        RB_TYPE_TO_DTYPE.fetch(data_type).to_s
      rescue KeyError
        raise ArgumentError, "Conversion of Ruby data type #{data_type} to Polars data type not implemented."
      end
    end

    def self._process_null_values(null_values)
      if null_values.is_a?(Hash)
        null_values.to_a
      else
        null_values
      end
    end

    def self._prepare_row_count_args(row_count_name = nil, row_count_offset = 0)
      if !row_count_name.nil?
        [row_count_name, row_count_offset]
      else
        nil
      end
    end

    def self.handle_projection_columns(columns)
      projection = nil
      if columns
        raise Todo
        # if columns.is_a?(String) || columns.is_a?(Symbol)
        #   columns = [columns]
        # elsif is_int_sequence(columns)
        #   projection = columns.to_a
        #   columns = nil
        # elsif !is_str_sequence(columns)
        #   raise ArgumentError, "columns arg should contain a list of all integers or all strings values."
        # end
      end
      [projection, columns]
    end

    def self.scale_bytes(sz, to:)
      scaling_factor = {
        "b" => 1,
        "k" => 1024,
        "m" => 1024 ** 2,
        "g" => 1024 ** 3,
        "t" => 1024 ** 4
      }[to[0]]
      if scaling_factor > 1
        sz / scaling_factor.to_f
      else
        sz
      end
    end

    def self.bool?(value)
      value.is_a?(TrueClass) || value.is_a?(FalseClass)
    end

    def self.strlike?(value)
      value.is_a?(String) || value.is_a?(Symbol)
    end

    def self.pathlike?(value)
      value.is_a?(String) || (defined?(Pathname) && value.is_a?(Pathname))
    end

    def self._is_iterable_of(val, eltype)
      val.all? { |x| x.is_a?(eltype) }
    end

    def self.is_bool_sequence(val)
      val.is_a?(Array) && val.all? { |x| x == true || x == false }
    end

    def self.is_dtype_sequence(val)
      val.is_a?(Array) && val.all? { |x| is_polars_dtype(x) }
    end

    def self.is_int_sequence(val)
      val.is_a?(Array) && _is_iterable_of(val, Integer)
    end

    def self.is_expr_sequence(val)
      val.is_a?(Array) && _is_iterable_of(val, Expr)
    end

    def self.is_rbexpr_sequence(val)
      val.is_a?(Array) && _is_iterable_of(val, RbExpr)
    end

    def self.is_str_sequence(val, allow_str: false)
      if allow_str == false && val.is_a?(String)
        false
      else
        val.is_a?(Array) && _is_iterable_of(val, String)
      end
    end

    def self.local_file?(file)
      Dir.glob(file).any?
    end
  end
end
