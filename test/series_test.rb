require_relative "test_helper"

class SeriesTest < Minitest::Test
  def test_new_int
    s = Polars::Series.new([1, 2, 3])
    assert_series [1, 2, 3], s, dtype: :i64
  end

  def test_new_float
    s = Polars::Series.new([1.0, 2, 3])
    assert_series [1, 2, 3], s, dtype: :f64
  end

  def test_new_string
    s = Polars::Series.new(["a", "b", "c"])
    assert_series ["a", "b", "c"], s, dtype: :str
  end

  def test_new_bool
    s = Polars::Series.new([true, false, true])
    assert_series [true, false, true], s, dtype: :bool
  end

  # def test_new_date
  #   dates = [Date.new(2022, 1, 1), Date.new(2022, 1, 2), Date.new(2022, 1, 3)]
  #   s = Polars::Series.new(dates)
  #   assert_series dates, s, dtype: :date
  # end

  # def test_new_datetime
  #   datetimes = [DateTime.new(2022, 1, 1), DateTime.new(2022, 1, 2), DateTime.new(2022, 1, 3)]
  #   s = Polars::Series.new(datetimes)
  #   assert_series datetimes, s, dtype: :date
  # end

  # def test_new_time
  #   times = [Time.new(2022, 1, 1), Time.new(2022, 1, 2), Time.new(2022, 1, 3)]
  #   s = Polars::Series.new(times)
  #   assert_series times, s, dtype: :datetime
  # end

  def test_new_nil
    s = Polars::Series.new([1, nil, 3])
    assert_series [1, nil, 3], s, dtype: :i64
  end

  def test_new_strict
    s = Polars::Series.new([1.0, "hello", 3], strict: false)
    assert_series [1, nil, 3], s, dtype: :f64

    s = Polars::Series.new([1, "hello", 3.5], strict: false)
    # assert_series [1, nil, nil], s
    assert_series [1, nil, 3], s, dtype: :i64
  end

  def test_new_unsupported
    error = assert_raises(ArgumentError) do
      Polars::Series.new("a", Object.new)
    end
    assert_equal "Series constructor called with unsupported type; got Object", error.message
  end

  def test_new_int_float
    skip
    s = Polars::Series.new([1, 2.5])
    assert_series [1, 2.5], s, dtype: :f64
  end

  def test_new_different
    error = assert_raises(TypeError) do
      Polars::Series.new([1, "hello", true])
    end
    assert_equal "no implicit conversion of String into Integer", error.message
  end

  def test_new_range
    s = Polars::Series.new(1..3)
    assert_series [1, 2, 3], s
  end

  def test_new_range_exclude_end
    s = Polars::Series.new(1...3)
    assert_series [1, 2], s
  end

  def test_new_range_endless
    error = assert_raises(RangeError) do
      Polars::Series.new(1..)
    end
    assert_equal "cannot get the last element of endless range", error.message
  end

  def test_dtype
    s = Polars::Series.new([1, 2, 3])
    assert_equal :i64, s.dtype
  end

  def test_flags
    s = Polars::Series.new([1, 2, 3])
    refute s.flags["SORTED_ASC"]
    refute s.flags["SORTED_DESC"]
    s.sort(in_place: true)
    assert s.flags["SORTED_ASC"]
    refute s.flags["SORTED_DESC"]
    s.sort(reverse: true, in_place: true)
    refute s.flags["SORTED_ASC"]
    assert s.flags["SORTED_DESC"]
  end

  def test_inner_dtype
    s = Polars::Series.new([1, 2, 3])
    assert_nil s.inner_dtype
  end

  def test_name
    s = Polars::Series.new("a", [1, 2, 3])
    assert_equal "a", s.name
  end

  def test_shape
    s = Polars::Series.new([1, 2, 3])
    assert_equal [3], s.shape
  end

  def test_to_s
    s = Polars::Series.new("a", [1, 2, 3])
    assert_match "Series: 'a' [i64]", s.to_s
  end

  def test_inspect
    s = Polars::Series.new("a", [1, 2, 3])
    assert_match "Series: 'a' [i64]", s.inspect
  end

  def test_bitand
    a = Polars::Series.new([true, true, false, false])
    b = Polars::Series.new([true, false, true, false])
    assert_series [true, false, false, false], a & b
  end

  def test_bitor
    a = Polars::Series.new([true, true, false, false])
    b = Polars::Series.new([true, false, true, false])
    assert_series [true, true, true, false], a | b
  end

  def test_bitxor
    a = Polars::Series.new([true, true, false, false])
    b = Polars::Series.new([true, false, true, false])
    assert_series [false, true, true, false], a ^ b
  end

  def test_math
    a = Polars::Series.new([10, 20, 30])
    b = Polars::Series.new([5, 10, 15])
    assert_series [15, 30, 45], a + b
    assert_series [5, 10, 15], a - b
    assert_series [50, 200, 450], a * b
    assert_series [2, 2, 2], a / b
  end

  def test_get
    s = Polars::Series.new(1..3)
    assert_equal 2, s[1]
    assert_series [1], s[0..0]
    assert_series [2], s[1..1]
    assert_series [3], s[2..2]
    assert_series [], s[3..3]
    assert_series [], s[-4..-4]
    assert_series [1], s[-3..-3]
    assert_series [2], s[-2..-2]
    assert_series [3], s[-1..-1]
    assert_series [1, 2], s[0..1]
    assert_series [1, 2], s[0...2]
    assert_series [1, 2], s[0..-2]
    assert_series [2, 3], s[1..-1]
    assert_series [1, 2], s[0...-1]
    assert_series [1, 2, 3], s[0..-1]
  end

  def test_set
    s = Polars::Series.new(1..3)
    s[1] = 9
    assert_series [1, 9, 3], s
  end

  def test_estimated_size
    s = Polars::Series.new(1..1000)
    assert_in_delta s.estimated_size("kb"), s.estimated_size / 1024.0
  end

  def test_sqrt
    s = Polars::Series.new([1, 4, 9])
    assert_series [1, 2, 3], s.sqrt
  end

  def test_any
    assert Polars::Series.new([false, false, true]).any
    refute Polars::Series.new([false, false, false]).any
  end

  def test_all
    assert Polars::Series.new([true, true, true]).all
    refute Polars::Series.new([true, true, false]).all
  end

  def test_sum
    assert_equal 6, Polars::Series.new([1, 2, 3]).sum
    assert_nil Polars::Series.new([]).sum
  end

  def test_mean
    assert_equal 2, Polars::Series.new([1, 2, 3]).mean
    assert_in_delta 0.75, Polars::Series.new([true, true, true, false]).mean
  end

  def test_product
    assert_equal 6, Polars::Series.new([1, 2, 3]).product
  end

  def test_min
    assert_equal 1, Polars::Series.new([1, 2, 3]).min
    assert_equal "a", Polars::Series.new(["a", "b", "c"]).min
  end

  def test_max
    assert_equal 3, Polars::Series.new([1, 2, 3]).max
    assert_equal "c", Polars::Series.new(["a", "b", "c"]).max
  end

  def test_std
    assert_equal 1, Polars::Series.new([1, 2, 3]).std
    assert_nil Polars::Series.new(["one", "two", "three"]).std
  end

  def test_var
    assert_equal 1, Polars::Series.new([1, 2, 3]).var
    assert_nil Polars::Series.new(["one", "two", "three"]).var
  end

  def test_median
    assert_in_delta 2, Polars::Series.new([1, 2, 9]).median
  end

  def test_quantile
    s = Polars::Series.new([1, 2, 3])
    assert_in_delta 1, s.quantile(0)
    assert_in_delta 2, s.quantile(0.5)
    assert_in_delta 3, s.quantile(1)

    error = assert_raises(ArgumentError) do
      Polars::Series.new([1, 2, 3]).quantile(2)
    end
    assert_equal "invalid quantile", error.message
  end

  # TODO improve
  def test_to_dummies
    s = Polars::Series.new(["a", "b", "b"])
    assert_equal [3, 2], s.to_dummies.shape
  end

  # TODO improve
  def test_value_counts
    s = Polars::Series.new(["a", "b", "b"])
    assert_equal [2, 2], s.value_counts.shape
  end

  def test_alias
    s = Polars::Series.new("a", [1, 2, 3])
    assert_equal "b", s.alias("b").name
  end

  def test_rename
    s = Polars::Series.new("a", [1, 2, 3])
    assert_equal "b", s.rename("b").name
    assert_equal "a", s.name
    s.rename("c", in_place: true)
    assert_equal "c", s.name
  end

  def test_chunk_lengths
    s = Polars::Series.new([1, 2, 3])
    assert_equal [3], s.chunk_lengths
  end

  def test_n_chunks
    s = Polars::Series.new([1, 2, 3])
    assert_equal 1, s.n_chunks
  end

  def test_cumsum
    s = Polars::Series.new([1, 2, 3])
    assert_series [1, 3, 6], s.cumsum
    assert_series [6, 5, 3], s.cumsum(reverse: true)
  end

  def test_cummin
    s = Polars::Series.new([1, 2, 3])
    assert_series [1, 1, 1], s.cummin
    assert_series [1, 2, 3], s.cummin(reverse: true)
  end

  def test_cummax
    s = Polars::Series.new([1, 2, 3])
    assert_series [1, 2, 3], s.cummax
    assert_series [3, 3, 3], s.cummax(reverse: true)
  end

  def test_cumprod
    s = Polars::Series.new([1, 2, 3])
    assert_series [1, 2, 6], s.cumprod
    assert_series [6, 6, 3], s.cumprod(reverse: true)
  end

  def test_limit
    s = Polars::Series.new([1, 2, 3])
    assert_series [1, 2], s.limit(2)
  end

  def test_slice
    s = Polars::Series.new([1, 2, 3, 4])
    assert_series [2, 3], s.slice(1, 2)
    assert_series [3, 4], s.slice(2)
  end

  def test_append
    a = Polars::Series.new([1, 2])
    b = Polars::Series.new([3, 4])
    a.append(b)
    assert_series [1, 2, 3, 4], a
  end

  def test_filter
    a = Polars::Series.new([1, 2, 3])
    b = Polars::Series.new([true, false, true])
    c = [false, true, true]
    assert_series [1, 3], a.filter(b)
    assert_series [2, 3], a.filter(c)
  end

  def test_head
    s = Polars::Series.new(1..20)
    assert_series 1..10, s.head
    assert_series [1, 2, 3], s.head(3)
  end

  def test_tail
    s = Polars::Series.new(1..20)
    assert_series 11..20, s.tail
    assert_series [18, 19, 20], s.tail(3)
  end

  def test_sort
    s = Polars::Series.new([2, 3, 1])
    assert_series [1, 2, 3], s.sort
    assert_series [3, 2, 1], s.sort(reverse: true)
    assert_series [2, 3, 1], s
  end

  def test_arg_min
    s = Polars::Series.new([1, 2, 3])
    assert_equal 0, s.arg_min
  end

  def test_arg_max
    s = Polars::Series.new([1, 2, 3])
    assert_equal 2, s.arg_max
  end

  def test_null_count
    s = Polars::Series.new([1, nil, nil, 4, nil])
    assert_equal 3, s.null_count
  end

  def test_has_validity
    refute Polars::Series.new([1, 2]).has_validity
    assert Polars::Series.new([1, nil]).has_validity
  end

  def test_is_empty
    assert Polars::Series.new([]).is_empty
    refute Polars::Series.new([1]).is_empty
    assert Polars::Series.new([]).empty?
    refute Polars::Series.new([1]).empty?
  end

  def test_series_equal
    a = Polars::Series.new([1, 2])
    b = Polars::Series.new([1, 2])
    assert a.series_equal(b)
  end

  def test_len
    assert_equal 10, Polars::Series.new(1..10).len
  end

  def test_to_a
    assert_equal [1, 2, 3], Polars::Series.new(1..3).to_a
  end

  def test_rechunk
    s = Polars::Series.new([1, 2, 3])
    s.rechunk
    s.rechunk(in_place: true)
  end

  def test_is_numeric
    assert Polars::Series.new([1]).is_numeric
    assert Polars::Series.new([1.0]).is_numeric
    refute Polars::Series.new(["one"]).is_numeric
    assert Polars::Series.new([1]).numeric?
    assert Polars::Series.new([1.0]).numeric?
    refute Polars::Series.new(["one"]).numeric?
  end

  def test_is_float
    assert Polars::Series.new([1.5]).is_float
    refute Polars::Series.new([1]).is_float
  end

  def test_is_bool
    assert Polars::Series.new([true]).is_bool
    refute Polars::Series.new([1]).is_bool
  end

  def test_is_utf8
    assert Polars::Series.new(["one"]).is_utf8
    refute Polars::Series.new([1]).is_utf8
  end

  def test_floor
    s = Polars::Series.new([1.12345, 2.56789, 3.901234])
    assert_series [1, 2, 3], s.floor
  end

  def test_ceil
    s = Polars::Series.new([1.12345, 2.56789, 3.901234])
    assert_series [2, 3, 4], s.ceil
  end

  def test_round
    s = Polars::Series.new([1.12345, 2.56789, 3.901234])
    assert_series [1.12, 2.57, 3.9], s.round(2)
    assert_series [1, 3, 4], s.round
  end

  def test_peak_max
    s = Polars::Series.new([1, 2, 3, 4, 5])
    assert_series [false, false, false, false, true], s.peak_max
  end

  def test_peak_min
    s = Polars::Series.new([4, 1, 3, 2, 5])
    assert_series [false, true, false, true, false], s.peak_min
  end

  def test_n_unique
    assert_equal 3, Polars::Series.new([1, 1, 2, 2, 5]).n_unique
  end

  def test_set_sorted
    s = Polars::Series.new([1, 2, 3])
    refute s.flags["SORTED_ASC"]
    assert s.set_sorted.flags["SORTED_ASC"]
  end
end
