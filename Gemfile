source "https://rubygems.org"

gemspec

gem "rake"
gem "rake-compiler"
gem "minitest"
gem "activerecord"
gem "numo-narray"
gem "vega"
gem "pg"
gem "mysql2"
gem "trilogy"
gem "sqlite3"

if ENV["TEST_DELTA"]
  gem "deltalake-rb", ">= 0.1.4"
end

if ENV["TEST_ICEBERG"]
  gem "iceberg", ">= 0.10.1"
end

# https://github.com/lsegal/yard/issues/1321
gem "yard", require: false
gem "rdoc", require: false
