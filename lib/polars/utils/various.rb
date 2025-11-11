module Polars
  module Utils
    def self._process_null_values(null_values)
      if null_values.is_a?(Hash)
        null_values.to_a
      else
        null_values
      end
    end

    def self._is_iterable_of(val, eltype)
      val.all? { |x| x.is_a?(eltype) }
    end

    def self.is_path_or_str_sequence(val)
      val.is_a?(::Array) && val.all? { |x| pathlike?(x) }
    end

    def self.is_bool_sequence(val, include_series: false)
      if include_series && val.is_a?(Series)
        return val.dtype == Boolean
      end
      val.is_a?(::Array) && val.all? { |x| x == true || x == false }
    end

    def self.is_int_sequence(val)
      val.is_a?(::Array) && _is_iterable_of(val, Integer)
    end

    def self.is_sequence(val, include_series: false)
      val.is_a?(::Array) || (include_series && val.is_a?(Series))
    end

    def self.is_str_sequence(val, allow_str: false)
      if allow_str == false && val.is_a?(::String)
        false
      else
        val.is_a?(::Array) && _is_iterable_of(val, ::String)
      end
    end

    def self.arrlen(obj)
      if obj.is_a?(Range)
        # size only works for numeric ranges
        obj.to_a.length
      elsif obj.is_a?(::String)
        nil
      else
        obj.length
      end
    rescue
      nil
    end

    def self.normalize_filepath(path, check_not_directory: true)
      path = File.expand_path(path) if !path.is_a?(::String) || path.start_with?("~")
      if check_not_directory && File.exist?(path) && Dir.exist?(path)
        raise ArgumentError, "Expected a file path; #{path} is a directory"
      end
      path
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

    def self._polars_warn(msg)
      warn msg
    end

    def self.extend_bool(value, n_match, value_name, match_name)
      values = bool?(value) ? [value] * n_match : value
      if n_match != values.length
        msg = "the length of `#{value_name}` (#{values.length}) does not match the length of `#{match_name}` (#{n_match})"
        raise ValueError, msg
      end
      values
    end

    def self.require_same_type(current, other)
      if !other.is_a?(current.class) && !current.is_a?(other.class)
        msg = (
          "expected `other` to be a #{current.inspect}, " +
          "not #{other.inspect}"
        )
        raise TypeError, msg
      end
    end

    def self._update_columns(df, new_columns)
      if df.width > new_columns.length
        cols = df.columns
        new_columns.each_with_index do |name, i|
          cols[i] = name
        end
        new_columns = cols
      end
      df.columns = new_columns.to_a
      df
    end

    def self.parse_percentiles(
      percentiles, inject_median: false
    )
      if percentiles.is_a?(Float)
        percentiles = [percentiles]
      elsif percentiles.nil?
        percentiles = []
      end
      if !percentiles.all? { |p| p >= 0 && p <= 1 }
        msg = "`percentiles` must all be in the range [0, 1]"
        raise ArgumentError, msg
      end

      sub_50_percentiles = percentiles.select { |p| p < 0.5 }.sort
      at_or_above_50_percentiles = percentiles.select { |p| p >= 0.5 }.sort

      if inject_median && (!at_or_above_50_percentiles || at_or_above_50_percentiles[0] != 0.5)
        at_or_above_50_percentiles = [0.5, *at_or_above_50_percentiles]
      end

      [*sub_50_percentiles, *at_or_above_50_percentiles]
    end
  end
end
