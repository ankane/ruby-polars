require_relative "test_helper"

class DocTest < Minitest::Test
  def test_series
    assert_docs Polars::Series
  end

  def test_data_frame
    assert_docs Polars::DataFrame
  end

  def test_lazy_frame
    assert_docs Polars::LazyFrame
  end

  def test_expr
    assert_docs Polars::Expr
  end

  def assert_docs(cls)
    @@once ||= YARD.parse || true

    in_temp_dir do
      P(cls.to_s).meths.each do |method|
        next if method.docstring.empty?

        assert_return(method)
        assert_examples(method)
      end
    end
  end

  def assert_return(method)
    if method.tags(:return).empty?
      raise "Missing @return tag (#{method.name})"
    end
  end

  def assert_examples(method)
    return if method.name == :is_datelike

    code = ""
    method.tags(:example).each do |example|
      # use variables from previous examples
      code += "\n" + example.text
      begin
        # just final output
        output = instance_eval(code)

        # print output
        if ENV["VERBOSE"]
          puts method.name
          p output
          puts
        end

        # check output
        lines = code.split("\n")
        if lines.last.start_with?("# => ")
          expected = lines.last[5..]
          assert_equal expected, output.inspect, "Example output (#{method.name})"
        elsif lines.last.start_with?("# ")
          expected = []
          while (line = lines.pop) && line != "# =>"
            expected << line[2..]
          end
          expected = expected.reverse.join("\n")
          output = output.inspect.gsub("\t", "        ")
          assert_equal expected, output, "Example output (#{method.name})"
        end
      rescue => e
        raise "Example failed (#{method.name}): #{e.message}"
      end
    end
  end
end
