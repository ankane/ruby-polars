require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "active_record"

logger = ActiveSupport::Logger.new(ENV["VERBOSE"] ? STDOUT : nil)

ActiveRecord::Base.logger = logger
ActiveRecord::Migration.verbose = ENV["VERBOSE"]

case ENV["ADAPTER"]
when "postgresql"
  ActiveRecord::Base.establish_connection adapter: "postgresql", database: "polars_ruby_test"
when "mysql"
  ActiveRecord::Base.establish_connection adapter: "mysql2", database: "polars_ruby_test"
else
  ActiveRecord::Base.establish_connection adapter: "sqlite3", database: ":memory:"
end

if ActiveSupport::VERSION::STRING.to_f == 8.0
  ActiveSupport.to_time_preserves_timezone = :zone
elsif ActiveSupport::VERSION::STRING.to_f == 7.2
  ActiveSupport.to_time_preserves_timezone = true
end

ActiveRecord::Schema.define do
  create_table :users, force: true do |t|
    t.string :name
    t.integer :number
    t.float :inexact
    t.boolean :active
    t.datetime :joined_at
    t.date :joined_on
    t.binary :bin
    t.decimal :dec, precision: 10, scale: 3
    t.text :txt
    t.time :joined_time
    if ENV["ADAPTER"] == "postgresql"
      t.column :settings, :jsonb
    else
      t.column :settings, :json
    end
  end
end

class User < ActiveRecord::Base
end

class Minitest::Test
  include Polars::Testing

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
    require "securerandom"

    # TODO clean up
    File.join(Dir.tmpdir, SecureRandom.alphanumeric(20))
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
end
