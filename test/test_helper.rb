require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"

class Minitest::Test
  include Polars::Testing

  def setup
    if stress?
      puts "#{self.class.name}##{name}"
      GC.stress = true
    end
  end

  def teardown
    GC.stress = false if stress?

    FileUtils.remove_entry(@temp_dir) if @temp_dir
  end

  def with_stress(value = true)
    return yield unless stress?

    previous_value = GC.stress
    begin
      GC.stress = value
      yield
    ensure
      GC.stress = previous_value
    end
  end

  def stress?
    ENV["STRESS"]
  end

  def require(name)
    with_stress(false) do
      super
    end
  end

  def assert_series(exp, act, dtype: nil, **options)
    assert_kind_of Polars::Series, act
    if exp.is_a?(Polars::Series)
      assert_series_equal(exp, act, **options)
    elsif exp.any? { |e| e.is_a?(Float) && e.nan? }
      assert exp.zip(act.to_a).all? { |e, a| e.nan? ? a.nan? : e == a }
    else
      assert_equal exp.to_a, act.to_a
    end
    assert_equal dtype, act.dtype if dtype
  end

  def assert_frame(exp, act, **options)
    exp = Polars::DataFrame.new(exp) if exp.is_a?(Hash)
    assert_frame_equal(exp, act, **options)
  end

  def assert_expr(act)
    assert_kind_of Polars::Expr, act
  end

  def temp_path
    File.join(temp_dir, "file")
  end

  # cleaned up in teardown
  def temp_dir
    @temp_dir ||= Dir.mktmpdir
  end

  def in_temp_dir
    Dir.mktmpdir do |dir|
      Dir.chdir(dir) do
        yield
      end
    end
  end

  def cloud?
    !cloud_prefix.nil?
  end

  def cloud_prefix
    ENV["CLOUD_PREFIX"]
  end

  def cloud_file(filename)
    "#{cloud_prefix}/#{filename}"
  end

  def active_support?
    ENV["ADAPTER"] != "none" && !windows?
  end

  def windows?
    Gem.win_platform?
  end
end
