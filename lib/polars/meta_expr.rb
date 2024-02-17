module Polars
  # Namespace for expressions on a meta level.
  class MetaExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # Equal.
    #
    # @return [Boolean]
    def ==(other)
      _rbexpr.meta_eq(other._rbexpr)
    end

    # Not equal.
    #
    # @return [Boolean]
    def !=(other)
      !(self == other)
    end

    # Indicate if this expression is the same as another expression.
    #
    # @return [Boolean]
    #
    # @example
    #   foo_bar = Polars.col("foo").alias("bar")
    #   foo = Polars.col("foo")
    #   foo_bar.meta.eq(foo)
    #   # => false
    #   foo_bar2 = Polars.col("foo").alias("bar")
    #   foo_bar.meta.eq(foo_bar2)
    #   # => true
    def eq(other)
      _rbexpr.meta_eq(other._rbexpr)
    end

    # Indicate if this expression is NOT the same as another expression.
    #
    # @return [Boolean]
    #
    # @example
    #   foo_bar = Polars.col("foo").alias("bar")
    #   foo = Polars.col("foo")
    #   foo_bar.meta.ne(foo)
    #   # => true
    #   foo_bar2 = Polars.col("foo").alias("bar")
    #   foo_bar.meta.ne(foo_bar2)
    #   # => false
    def ne(other)
      !eq(other)
    end

    # Indicate if this expression expands into multiple expressions.
    #
    # @return [Boolean]
    #
    # @example
    #   e = Polars.col(["a", "b"]).alias("bar")
    #   e.meta.has_multiple_outputs
    #   # => true
    def has_multiple_outputs
      _rbexpr.meta_has_multiple_outputs
    end

    # Indicate if this expression is a basic (non-regex) unaliased column.
    #
    # @return [Boolean]
    #
    # @example
    #   e = Polars.col("foo")
    #   e.meta.is_column
    #   # => true
    #   e = Polars.col("foo") * Polars.col("bar")
    #   e.meta.is_column
    #   # => false
    #   e = Polars.col("^col.*\d+$")
    #   e.meta.is_column
    #   # => false
    def is_column
      _rbexpr.meta_is_column
    end

    # Indicate if this expression expands to columns that match a regex pattern.
    #
    # @return [Boolean]
    #
    # @example
    #   e = Polars.col("^.*$").alias("bar")
    #   e.meta.is_regex_projection
    #   # => true
    def is_regex_projection
      _rbexpr.meta_is_regex_projection
    end

    # Get the column name that this expression would produce.
    #
    # @return [String]
    def output_name
      _rbexpr.meta_output_name
    end

    # Pop the latest expression and return the input(s) of the popped expression.
    #
    # @return [Array]
    def pop
      _rbexpr.meta_pop.map { |e| Utils.wrap_expr(e) }
    end

    # Get a list with the root column name.
    #
    # @return [Array]
    def root_names
      _rbexpr.meta_roots
    end

    # Undo any renaming operation like `alias` or `keep_name`.
    #
    # @return [Expr]
    def undo_aliases
      Utils.wrap_expr(_rbexpr.meta_undo_aliases)
    end
  end
end
