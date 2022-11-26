module Polars
  # @private
  class Slice
    def initialize(obj)
      @obj = obj
    end

    # Apply a slice operation, taking advantage of any potential fast paths.
    def apply(s)
      # normalize slice
      _slice_setup(s)

      # check for fast-paths / single-operation calls
      if @slice_length == 0
        @obj.cleared
      elsif @is_unbounded && [-1, 1].include?(@stride)
        @stride < 0 ? @obj.reverse : @obj.clone
      elsif @start >= 0 && @stop >= 0 && @stride == 1
        @obj.slice(@start, @slice_length)
      elsif @stride < 0 && @slice_length == 1
        @obj.slice(@stop + 1, 1)
      else
        # multi-operation calls; make lazy
        lazyobj = _lazify(@obj)
        sliced = @stride > 0 ? _slice_positive(lazyobj) : _slice_negative(lazyobj)
        _as_original(sliced, @obj)
      end
    end

    private

    # Return lazy variant back to its original type.
    def _as_original(lazy, original)
      frame = lazy.collect
      original.is_a?(DataFrame) ? frame : frame.to_series
    end

    # Make lazy to ensure efficient/consistent handling.
    def _lazify(obj)
      obj.is_a?(DataFrame) ? obj.lazy : obj.to_frame.lazy
    end

    # Logic for slices with positive stride.
    def _slice_positive(obj)
      # note: at this point stride is guaranteed to be > 1
      obj.slice(@start, @slice_length).take_every(@stride)
    end

    # Logic for slices with negative stride.
    def _slice_negative(obj)
      stride = @stride.abs
      lazyslice = obj.slice(@stop + 1, @slice_length).reverse
      stride > 1 ? lazyslice.take_every(stride) : lazyslice
    end

    # Normalize slice bounds, identify unbounded and/or zero-length slices.
    def _slice_setup(s)
      # can normalize slice indices as we know object size
      obj_len = @obj.len
      start = if s.begin
        if s.begin < 0
          [s.begin + obj_len, 0].max
        else
          s.begin
        end
      else
        0
      end
      stop = if s.end
        if s.end < 0
          s.end + (s.exclude_end? ? 0 : 1) + obj_len
        else
          s.end + (s.exclude_end? ? 0 : 1)
        end
      else
        obj_len
      end
      stride = 1

      # check if slice is actually unbounded
      if stride >= 1
        @is_unbounded = start <= 0 && stop >= obj_len
      else
        @is_unbounded = stop == -1 && start >= obj_len - 1
      end

      # determine slice length
      if @obj.is_empty
        @slice_length = 0
      elsif @is_unbounded
        @slice_length = obj_len
      else
        @slice_length = if start == stop || (stride > 0 && start > stop) || (stride < 0 && start < stop)
          0
        else
          (stop - start).abs
        end
      end
      @start = start
      @stop = stop
      @stride = stride
    end
  end
end
