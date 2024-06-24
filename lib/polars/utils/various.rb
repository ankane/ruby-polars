module Polars
  module Utils
    def self._is_iterable_of(val, eltype)
      val.all? { |x| x.is_a?(eltype) }
    end

    def self.is_bool_sequence(val)
      val.is_a?(::Array) && val.all? { |x| x == true || x == false }
    end

    def self.is_int_sequence(val)
      val.is_a?(::Array) && _is_iterable_of(val, Integer)
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
      path = File.expand_path(path)
      if check_not_directory && File.exist?(path) && Dir.exist?(path)
        raise ArgumentError, "Expected a file path; #{path} is a directory"
      end
      path
    end
  end
end
