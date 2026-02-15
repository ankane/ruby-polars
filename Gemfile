source "https://rubygems.org"

gemspec

gem "rake"
gem "rake-compiler"
gem "minitest"
gem "activerecord"
gem "numo-narray-alt"
gem "vega"
gem "pg", require: false
gem "mysql2", require: false, platform: :ruby
gem "trilogy", require: false, platform: :ruby
gem "sqlite3", require: false
gem "ruby_memcheck"

if ENV["TEST_DELTA"]
  gem "deltalake-rb", ">= 0.1.4"
end

if ENV["TEST_ICEBERG"]
  gem "iceberg", ">= 0.10.2"
end

# https://github.com/lsegal/yard/issues/1321
gem "yard", require: false
gem "rdoc", require: false
