require "bundler/gem_tasks"
require "rake/testtask"
require "rake/extensiontask"

task default: :test
Rake::TestTask.new do |t|
  t.libs << "test"
  t.test_files = FileList["test/**/*_test.rb"].exclude(/docs_test/)
end

Rake::TestTask.new("test:docs") do |t|
  t.libs << "test"
  t.pattern = "test/docs_test.rb"
end

platforms = [
  "x86_64-linux",
  "aarch64-linux",
  "x86_64-darwin",
  "arm64-darwin",
  "x64-mingw-ucrt",
  "x64-mingw32"
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
  path = "lib/polars/polars.bundle"
  File.unlink(path) if File.exist?(path)
end

Rake::Task["build"].enhance [:remove_ext]
