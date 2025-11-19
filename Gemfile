source "https://rubygems.org"

gemspec

gem "rake"
gem "rake-compiler"
gem "minitest"
gem "activerecord"
gem "vega"
gem "pg", require: false
gem "mysql2", require: false, platform: :ruby
gem "trilogy", require: false, platform: :ruby
gem "sqlite3", require: false

# TODO remove when numo-narray > 0.9.2.1 is released
if Gem.win_platform?
  gem "numo-narray", github: "ruby-numo/numo-narray", ref: "421feddb46cac5145d69067fc1ac3ba3c434f668"
else
  gem "numo-narray"
end

if ENV["TEST_DELTA"]
  gem "deltalake-rb", ">= 0.1.4"
end

if ENV["TEST_ICEBERG"]
  gem "iceberg", ">= 0.10.2"
end

# https://github.com/lsegal/yard/issues/1321
gem "yard", require: false
gem "rdoc", require: false
