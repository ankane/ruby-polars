require "bundler/gem_tasks"
require "rake/testtask"
require "rake/extensiontask"
require "ruby_memcheck"

Rake::TestTask.new do |t|
  t.test_files = FileList["test/**/*_test.rb"].exclude(/docs_test/)
end

namespace :test do
  RubyMemcheck::TestTask.new(:valgrind) do |t|
    # TODO test docs
    t.test_files = FileList["test/**/*_test.rb"].exclude(/docs_test/)
  end
end

task default: :test

Rake::TestTask.new("test:docs") do |t|
  t.pattern = "test/docs_test.rb"
end

platforms = [
  "x86_64-linux",
  "x86_64-linux-musl",
  "aarch64-linux",
  "aarch64-linux-musl",
  "x86_64-darwin",
  "arm64-darwin",
  "x64-mingw-ucrt"
]

gemspec = Bundler.load_gemspec("polars-df.gemspec")
Rake::ExtensionTask.new("polars", gemspec) do |ext|
  ext.lib_dir = "lib/polars"
  ext.cross_compile = true
  ext.cross_platform = platforms
  ext.cross_compiling do |spec|
    spec.dependencies.reject! { |dep| dep.name == "rb_sys" }
    spec.files.reject! { |file| File.fnmatch?("ext/*", file, File::FNM_EXTGLOB) }
  end
end

task :remove_ext do
  Dir["lib/polars/polars.{bundle,so}"].each do |path|
    File.unlink(path)
  end
end

Rake::Task["build"].enhance [:remove_ext]
