source "https://rubygems.org"

gemspec

gem "rake"
gem "rake-compiler"
gem "minitest"
gem "activerecord"
gem "numo-narray"
gem "vega"

case ENV["ADAPTER"]
when "postgresql"
  gem "pg"
when "mysql"
  gem "mysql2"
else
  gem "sqlite3"
end

if ENV["TEST_DELTA"]
  gem "deltalake-rb", ">= 0.1.4"
end

# https://github.com/lsegal/yard/issues/1321
gem "yard", require: false
