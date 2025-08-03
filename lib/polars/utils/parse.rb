module Polars
  module Utils
    def self.parse_into_expression(
      input,
      str_as_lit: false,
      list_as_series: false,
      structify: false,
      dtype: nil
    )
      if input.is_a?(Expr)
        expr = input
        if structify
          expr = _structify_expression(expr)
        end
      elsif (input.is_a?(::String) || input.is_a?(Symbol)) && !str_as_lit
        expr = F.col(input)
      elsif input.is_a?(::Array) && list_as_series
        expr = F.lit(Series.new(input), dtype: dtype)
      else
        expr = F.lit(input, dtype: dtype)
      end

      expr._rbexpr
    end

    def self.parse_into_list_of_expressions(*inputs, __structify: false, **named_inputs)
      exprs = _parse_positional_inputs(inputs, structify: __structify)
      if named_inputs.any?
        named_exprs = _parse_named_inputs(named_inputs, structify: __structify)
        exprs.concat(named_exprs)
      end

      exprs
    end

    def self.parse_into_selector(i, strict: true)
      if i.is_a?(::String)
        cs = Selectors

        cs.by_name([i], require_all: strict)
      elsif i.is_a?(Selector)
        i
      elsif i.is_a?(Expr)
        i.meta.as_selector
      else
        msg = "cannot turn #{i.inspect} into selector"
        raise TypeError, msg
      end
    end

    def self.parse_list_into_selector(inputs, strict: true)
      if inputs.is_a?(::Array)
        cs = Selectors

        columns = inputs.select { |i| i.is_a?(::String) }
        selector = cs.by_name(columns, require_all: strict)

        if columns.length == inputs.length
          return selector
        end

        # A bit cleaner
        if columns.length == 0
          selector = cs.empty
        end

        inputs.each do |i|
          selector |= parse_into_selector(i, strict: strict)
        end
        selector
      else
        parse_into_selector(inputs, strict: strict)
      end
    end

    def self._parse_positional_inputs(inputs, structify: false)
      inputs_iter = _parse_inputs_as_iterable(inputs)
      inputs_iter.map { |e| parse_into_expression(e, structify: structify) }
    end

    def self._parse_inputs_as_iterable(inputs)
      if inputs.empty?
        return []
      end

      if inputs.length == 1 && inputs[0].is_a?(::Array)
        return inputs[0]
      end

      inputs
    end

    def self._parse_named_inputs(named_inputs, structify: false)
      named_inputs.map do |name, input|
        parse_into_expression(input, structify: structify)._alias(name.to_s)
      end
    end

    def self.parse_predicates_constraints_into_expression(*predicates, **constraints)
      all_predicates = _parse_positional_inputs(predicates)

      if constraints.any?
        constraint_predicates = _parse_constraints(constraints)
        all_predicates.concat(constraint_predicates)
      end

      _combine_predicates(all_predicates)
    end

    def self._parse_constraints(constraints)
      constraints.map do |name, value|
        Polars.col(name).eq(value)._rbexpr
      end
    end

    def self._combine_predicates(predicates)
      if !predicates.any?
        msg = "at least one predicate or constraint must be provided"
        raise TypeError, msg
      end

      if predicates.length == 1
        return predicates[0]
      end

      Plr.all_horizontal(predicates)
    end
  end
end
