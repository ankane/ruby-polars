require_relative "test_helper"

class SeriesTest < Minitest::Test
  def test_new_int
    s = Polars::Series.new([1, 2, 3])
    assert_series [1, 2, 3], s, dtype: Polars::Int64
  end

  def test_new_float
    s = Polars::Series.new([1.0, 2, 3])
    assert_series [1, 2, 3], s, dtype: Polars::Float64
  end

  def test_new_string
    s = Polars::Series.new(["a", "b", "c"])
    assert_series ["a", "b", "c"], s, dtype: Polars::String
  end

  def test_new_binary
    s = Polars::Series.new(["a".b, "b".b, "c".b])
    assert_series ["a", "b", "c"], s, dtype: Polars::Binary
  end

  def test_new_bool
    s = Polars::Series.new([true, false, true])
    assert_series [true, false, true], s, dtype: Polars::Boolean
  end

  def test_new_date
    dates = [Date.new(2022, 1, 1), Date.new(2022, 1, 2), Date.new(2022, 1, 3)]
    s = Polars::Series.new(dates)
    assert_series dates, s, dtype: Polars::Date
  end

  def test_new_datetime
    s = Polars::Series.new([DateTime.new(2022, 1, 1), DateTime.new(2022, 1, 2), DateTime.new(2022, 1, 3)])
    assert_series [Time.utc(2022, 1, 1), Time.utc(2022, 1, 2), Time.utc(2022, 1, 3)], s, dtype: Polars::Datetime.new("ns")
  end

  def test_new_time
    times = [Time.new(2022, 1, 1), Time.new(2022, 1, 2), Time.new(2022, 1, 3)]
    s = Polars::Series.new(times)
    assert_series times, s, dtype: Polars::Datetime.new("ns")
  end

  def test_new_time_with_zone
    times = [Time.new(2022, 1, 1), Time.new(2022, 1, 2), Time.new(2022, 1, 3)].map { |v| v.in_time_zone("Eastern Time (US & Canada)") }
    s = Polars::Series.new(times)
    assert_series times, s, dtype: Polars::Datetime.new("ns")
  end

  def test_new_all_nil
    s = Polars::Series.new([nil, nil, nil])
    assert_series [nil, nil, nil], s, dtype: Polars::Null
  end

  def test_new_some_nil
    s = Polars::Series.new([1, nil, 3])
    assert_series [1, nil, 3], s, dtype: Polars::Int64
  end

  def test_new_struct
    s = Polars::Series.new([{"f1" => 1}, {"f1" => 2}])
    assert_series [{"f1" => 1}, {"f1" => 2}], s, dtype: Polars::Struct
    assert_equal({"f1" => Polars::Int64}, s.dtype.to_schema)
    assert_series [1, 2], s.struct["f1"], dtype: Polars::Int64
  end

  def test_new_struct_nested
    data = [
      {"a" => {"b" => {"c" => 1}}},
      {"a" => {"b" => {"c" => 2}}}
    ]
    s = Polars::Series.new(data)
    assert_kind_of Polars::Struct, s.dtype
    assert_series [1, 2], s.struct["a"].struct["b"].struct["c"], dtype: Polars::Int64
  end

  def test_new_list
    s = Polars::Series.new([[1, 2, 3], [5]])
    assert_series [[1, 2, 3], [5]], s, dtype: Polars::List
    assert s.flags.key?("FAST_EXPLODE")
  end

  def test_new_list_of_structs
    s = Polars::Series.new([[{}], [{}], [{}]])
    assert_series [[{"" => nil}], [{"" => nil}], [{"" => nil}]], s, dtype: Polars::List
  end

  def test_new_strict
    s = Polars::Series.new([1.0, "hello", 3], strict: false)
    assert_series [1, nil, 3], s, dtype: Polars::Float64

    s = Polars::Series.new([1, "hello", 3.5], strict: false)
    assert_series [1, nil, 3.5], s, dtype: Polars::Float64
  end

  def test_new_enum
    dtype = Polars::Enum.new(["a", "b"])
    s = Polars::Series.new([nil, "a", "b"], dtype: dtype)
    assert_series [nil, "a", "b"], s, dtype: dtype
  end

  def test_new_bigdecimal
    s = Polars::Series.new([BigDecimal("1"), nil, BigDecimal("3")])
    assert_series [BigDecimal("1"), nil, BigDecimal("3")], s, dtype: Polars::Decimal
  end

  def test_new_bigdecimal_too_large
    error = assert_raises do
      Polars::Series.new([BigDecimal("-99999999999999999999.9999999999999999999")])
    end
    assert_equal "BigDecimal is too large to fit in Decimal128", error.message
  end

  def test_new_object
    s = Polars::Series.new([Rational(1), nil, Rational(2, 4)])
    assert_series [Rational(1), nil, Rational(2, 4)], s, dtype: Polars::Object
  end

  def test_new_empty
    s = Polars::Series.new([])
    assert_series [], s, dtype: Polars::Float32
  end

  def test_new_unsupported
    error = assert_raises(ArgumentError) do
      Polars::Series.new("a", Object.new)
    end
    assert_equal "Series constructor called with unsupported type; got Object", error.message
  end

  def test_new_int_float
    s = Polars::Series.new([1, 2.5])
    assert_series [1, 2.5], s, dtype: Polars::Float64
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

  def test_new_range_string
    s = Polars::Series.new("a".."c")
    assert_series ["a", "b", "c"], s
  end

  def test_duration
    today = Date.today
    s = Polars::Series.new([today - 2, today - 1, today]) - (today - 3)
    sec = 86400
    assert_equal [sec, 2 * sec, 3 * sec], s.to_a
    error = assert_raises(ArgumentError) do
      s / sec.to_f
    end
    assert_equal "first cast to integer before dividing datelike dtypes", error.message
  end

  def test_dtype
    s = Polars::Series.new([1, 2, 3], dtype: Polars::Int8)
    assert_equal Polars::Int8, s.dtype
    refute s.dtype.eql?(Polars::Int8)
    assert_kind_of Polars::Int8, s.dtype
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

  def test_not
    a = Polars::Series.new([true, false])
    assert_series [false, true], !a
  end

  def test_comp_series
    a = Polars::Series.new([1, 2, 3, 4])
    b = Polars::Series.new([0, 2, 3, 5])
    assert_series [false, true, true, false], a == b
    assert_series [true, false, false, true], a != b
    assert_series [true, false, false, false], a > b
    assert_series [false, false, false, true], a < b
    assert_series [true, true, true, false], a >= b
    assert_series [false, true, true, true], a <= b
  end

  def test_comp_scalar
    a = Polars::Series.new([1, 2, 3])
    assert_series [false, true, false], a == 2
    assert_series [true, false, true], a != 2
    assert_series [false, false, true], a > 2
    assert_series [true, false, false], a < 2
    assert_series [false, true, true], a >= 2
    assert_series [true, true, false], a <= 2
  end

  def test_equals_nan
    s = Polars::Series.new([1.0, Float::NAN, Float::INFINITY])
    assert_series [true, true, true], (s == s)
  end

  def test_arithmetic
    a = Polars::Series.new([10, 20, 30])
    b = Polars::Series.new([5, 10, 15])
    assert_series [15, 30, 45], a + b
    assert_series [5, 10, 15], a - b
    assert_series [50, 200, 450], a * b
    assert_series [2, 2, 2], a / b
    assert_series [0, 0, 0], a % b
    assert_series [15, 25, 35], a + 5
    assert_series [15, 25, 35], 5 + a
    assert_series [5, 15, 25], a - 5
    assert_series [25, 15, 5], 35 - a
    assert_series [50, 100, 150], 5 * a
    assert_series [50, 100, 150], a * 5
    assert_series [5, 5, 5], 5 % a
    assert_series [0, 0, 0], a % 5
  end

  def test_pow
    a = Polars::Series.new([10, 20, 30])
    assert_series [100, 400, 900], a**2
  end

  def test_negation
    a = Polars::Series.new([10, 20, 30])
    assert_series [-10, -20, -30], -a
  end

  def test_get
    s = Polars::Series.new(1..3)
    assert_equal 2, s[1]
    assert_equal 3, s[-1]
    assert_series [1, 2], s[[0, 1]]
    assert_series [1, 2], s[Polars::Series.new([0, 1])]
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
    s[[0, 2]] = 2
    assert_series [2, 9, 2], s
    s[1..2] = 4
    assert_series [2, 4, 4], s
    s[1...2] = 5
    assert_series [2, 5, 4], s
    s[[0, 1]] = [7, 8]
    assert_series [7, 8, 4], s
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
    assert Polars::Series.new([false, false, true]).any?
    refute Polars::Series.new([false, false, false]).any?
    assert Polars::Series.new([1, 2, 3]).any?(&:even?)
    refute Polars::Series.new([1, 3, 5]).any?(&:even?)
  end

  def test_all
    assert Polars::Series.new([true, true, true]).all?
    refute Polars::Series.new([true, true, false]).all?
    refute Polars::Series.new([1, 2, 3]).all?(&:even?)
    assert Polars::Series.new([2, 4, 6]).all?(&:even?)
    assert Polars::Series.new([true, nil]).all?
    refute Polars::Series.new([true, nil]).all?(ignore_nulls: false)
  end

  def test_none
    assert Polars::Series.new([false, false, false]).none?
    refute Polars::Series.new([false, true, false]).none?
    assert Polars::Series.new([1, 3, 5]).none?(&:even?)
    refute Polars::Series.new([2, 3, 5]).none?(&:even?)
  end

  def test_log
    s = Polars::Series.new([1, 2, 4])
    assert_series [0, 1, 2], s.log(2)
  end

  def test_log10
    s = Polars::Series.new([1, 10, 100])
    assert_series [0, 1, 2], s.log10
  end

  def test_exp
    s = Polars::Series.new([0, 1])
    assert_series [1, Math::E], s.exp
  end

  def test_drop_nulls
    s = Polars::Series.new([nil, 1.0, Float::NAN])
    assert_series [1.0, Float::NAN], s.drop_nulls
  end

  def test_drop_nans
    s = Polars::Series.new([nil, 1.0, Float::NAN])
    assert_series [nil, 1.0], s.drop_nans
  end

  def test_sum
    assert_equal 6, Polars::Series.new([1, 2, 3]).sum
    assert_in_delta 0, Polars::Series.new([]).sum
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

  def test_nan_max
    assert_predicate Polars::Series.new([1.0, Float::NAN, 3.0]).nan_max, :nan?
    # TODO debug on CI
    # assert_in_delta 3.0, Polars::Series.new([1.0, 2.0, 3.0]).nan_max
  end

  def test_nan_min
    assert_predicate Polars::Series.new([1.0, Float::NAN, 3.0]).nan_min, :nan?
    # TODO debug on CI
    # assert_in_delta 1.0, Polars::Series.new([1.0, 2.0, 3.0]).nan_min
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

    error = assert_raises(RuntimeError) do
      Polars::Series.new([1, 2, 3]).quantile(2)
    end
    assert_equal "quantile should be between 0.0 and 1.0", error.message
  end

  # TODO improve
  def test_to_dummies
    s = Polars::Series.new(["a", "b", "b"])
    assert_equal [3, 2], s.to_dummies.shape
  end

  def test_value_counts
    s = Polars::Series.new("a", ["x", "x", "y"])
    expected = Polars::DataFrame.new({"a" => ["x", "y"], "count" => [2, 1]})
    assert_frame expected, s.value_counts, check_row_order: false, check_dtype: false
  end

  def test_entropy
    a = Polars::Series.new([0.99, 0.005, 0.005])
    assert_in_delta 0.06293300616044681, a.entropy(normalize: true)

    b = Polars::Series.new([0.65, 0.10, 0.25])
    assert_in_delta 0.8568409950394724, b.entropy(normalize: true)
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

  def test_cum_sum
    s = Polars::Series.new([1, 2, 3])
    assert_series [1, 3, 6], s.cum_sum
    assert_series [6, 5, 3], s.cum_sum(reverse: true)
  end

  def test_cum_min
    s = Polars::Series.new([1, 2, 3])
    assert_series [1, 1, 1], s.cum_min
    assert_series [1, 2, 3], s.cum_min(reverse: true)
  end

  def test_cum_max
    s = Polars::Series.new([1, 2, 3])
    assert_series [1, 2, 3], s.cum_max
    assert_series [3, 3, 3], s.cum_max(reverse: true)
  end

  def test_cum_prod
    s = Polars::Series.new([1, 2, 3])
    assert_series [1, 2, 6], s.cum_prod
    assert_series [6, 6, 3], s.cum_prod(reverse: true)
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

  def test_search_sorted
    s = Polars::Series.new([1, 2, 4])
    assert_equal 2, s.search_sorted(3)
  end

  def test_take
    s = Polars::Series.new("a", [1, 2, 3, 4])
    assert_series [2, 4], s.take([1, 3])
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

  def test_reverse
    s = Polars::Series.new([1, 2, 3])
    assert_series [3, 2, 1], s.reverse
  end

  def test_is_numeric
    assert Polars::Series.new([1]).is_numeric
    assert Polars::Series.new([1.0]).is_numeric
    refute Polars::Series.new(["one"]).is_numeric
    assert Polars::Series.new([1]).numeric?
    assert Polars::Series.new([1.0]).numeric?
    refute Polars::Series.new(["one"]).numeric?
  end

  def test_is_datelike
    assert Polars::Series.new([Date.today]).is_datelike
    assert Polars.date_range(DateTime.new(2020), DateTime.new(2023), "1y").is_datelike
    refute Polars::Series.new([1]).is_datelike
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

  def test_fill_nan
    s = Polars::Series.new("a", [1.0, 2.0, 3.0, Float::NAN])
    assert_series [1.0, 2.0, 3.0, 0.0], s.fill_nan(0)
  end

  def test_fill_null
    s = Polars::Series.new("a", [1, 2, 3, nil])
    assert_series [1, 2, 3, 3], s.fill_null(strategy: "forward")
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

  def test_unique
    assert_series [1, 2, 5], Polars::Series.new([1, 1, 2, 2, 5]).unique.sort
    assert_series [1, 2, 5], Polars::Series.new([1, 1, 2, 2, 5]).uniq.sort
  end

  def test_reinterpret
    s = Polars::Series.new("a", [2**64 - 1, 0, 1], dtype: Polars::UInt64)
    assert_series [-1, 0, 1], s.reinterpret
  end

  def test_interpolate
    s = Polars::Series.new("a", [1, 2, nil, nil, 5])
    assert_series [1, 2, 3, 4, 5], s.interpolate
  end

  def test_shrink_to_fit
    s = Polars::Series.new([1, 2, 3])
    s.shrink_to_fit
    s.shrink_to_fit(in_place: true)
  end

  def test_skew
    s = Polars::Series.new([1, 2, 3])
    assert_in_delta 0, s.skew
  end

  def test_kurtosis
    s = Polars::Series.new([1, 2, 3])
    assert_in_delta (-1.5), s.kurtosis
    assert_in_delta 1.5, s.kurtosis(fisher: false)
  end

  def test_clip
    s = Polars::Series.new("foo", [-50, 5, nil, 50])
    assert_series [1, 5, nil, 10], s.clip(1, 10)
  end

  def test_clip_min
    s = Polars::Series.new("foo", [-50, 5, nil, 50])
    assert_series [1, 5, nil, 50], s.clip_min(1)
  end

  def test_clip_max
    s = Polars::Series.new("foo", [-50, 5, nil, 50])
    assert_series [-50, 5, nil, 10], s.clip_max(10)
  end

  def test_reshape
    s = Polars::Series.new([1, 2, 3, 4])
    s.reshape([2, -1])
  end

  def test_shuffle
    s = Polars::Series.new("a", [1, 2, 3])
    assert_series [2, 1, 3], s.shuffle(seed: 1)
  end

  def test_extend_constant
    s = Polars::Series.new("a", [1, 2, 3])
    assert_series [1, 2, 3, 99, 99], s.extend_constant(99, 2)
  end

  def test_extend_constant_nil
    s = Polars::Series.new("a", [1, 2, 3])
    assert_series [1, 2, 3, nil, nil], s.extend_constant(nil, 2)
  end

  def test_set_sorted
    s = Polars::Series.new([1, 2, 3])
    refute s.flags["SORTED_ASC"]
    assert s.set_sorted.flags["SORTED_ASC"]
  end

  def test_new_from_index
    s = Polars::Series.new([1, 2, 3])
    assert_series [2, 2, 2, 2, 2], s.new_from_index(1, 5)
  end

  def test_shrink_dtype
    s = Polars::Series.new([1, 2, 3])
    assert_series [1, 2, 3], s.shrink_dtype, dtype: Polars::Int8
  end

  def test_apply
    s = Polars::Series.new([1, 2, 3])
    assert_series [1, 4, 9], s.apply { |v| v**2 }, dtype: Polars::Int64
    assert_series [1, 2, 3], s.map(&:to_f), dtype: Polars::Float64
    assert_series [false, true, false], s.map(&:even?), dtype: Polars::Boolean
  end
end
