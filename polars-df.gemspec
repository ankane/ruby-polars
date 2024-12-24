require_relative "lib/polars/version"

Gem::Specification.new do |spec|
  spec.name          = "polars-df"
  spec.version       = Polars::VERSION
  spec.summary       = "Blazingly fast DataFrames for Ruby"
  spec.homepage      = "https://github.com/ankane/ruby-polars"
  spec.license       = "MIT"

  spec.author        = "Andrew Kane"
  spec.email         = "andrew@ankane.org"

  spec.files         = Dir["*.{md,txt}", "{ext,lib}/**/*", "Cargo.*", ".yardopts"]
  spec.require_path  = "lib"
  spec.extensions    = ["ext/polars/extconf.rb"]

  spec.required_ruby_version = ">= 3.2"

  spec.add_dependency "bigdecimal"
  spec.add_dependency "rb_sys"
end
