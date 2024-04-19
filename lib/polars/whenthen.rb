module Polars
  # @private
  class When
    attr_accessor :_when

    def initialize(rbwhen)
      self._when = rbwhen
    end

    def then(statement)
      statement_rbexpr = Utils.parse_as_expression(statement)
      Then.new(_when.then(statement_rbexpr))
    end
  end

  # @private
  class Then < Expr
    attr_accessor :_then

    def initialize(rbthen)
      self._then = rbthen
    end

    def self._from_rbexpr(rbexpr)
      Utils.wrap_expr(rbexpr)
    end

    def _rbexpr
      _then.otherwise(Polars.lit(nil)._rbexpr)
    end

    def when(*predicates, **constraints)
      condition_rbexpr = Utils.parse_when_inputs(*predicates, **constraints)
      ChainedWhen.new(_then.when(condition_rbexpr))
    end

    def otherwise(statement)
      statement_rbexpr = Utils.parse_as_expression(statement)
      Utils.wrap_expr(_then.otherwise(statement_rbexpr))
    end
  end

  # @private
  class ChainedWhen
    attr_accessor :_chained_when

    def initialize(chained_when)
      self._chained_when = chained_when
    end

    def then(statement)
      statement_rbexpr = Utils.parse_as_expression(statement)
      ChainedThen.new(_chained_when.then(statement_rbexpr))
    end
  end

  # @private
  class ChainedThen < Expr
    attr_accessor :_chained_then

    def initialize(chained_then)
      self._chained_then = chained_then
    end

    def self._from_rbexpr(rbexpr)
      Utils.wrap_expr(rbexpr)
    end

    def _rbexpr
      _chained_then.otherwise(Polars.lit(nil)._rbexpr)
    end

    def when(*predicates, **constraints)
      condition_rbexpr = Utils.parse_when_inputs(*predicates, **constraints)
      ChainedWhen.new(_chained_then.when(condition_rbexpr))
    end

    def otherwise(statement)
      statement_rbexpr = Utils.parse_as_expression(statement)
      Utils.wrap_expr(_chained_then.otherwise(statement_rbexpr))
    end
  end
end
