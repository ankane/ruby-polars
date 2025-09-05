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

    # Indicate if this expression only selects columns (optionally with aliasing).
    #
    # This can include bare columns, columns matched by regex or dtype, selectors
    # and exclude ops, and (optionally) column/expression aliasing.
    #
    # @param allow_aliasing [Boolean]
    #   If false (default), any aliasing is not considered to be column selection.
    #   Set true to allow for column selection that also includes aliasing.
    #
    # @return [Boolean]
    #
    # @example
    #   e = Polars.col("foo")
    #   e.meta.is_column_selection
    #   # => true
    #
    # @example
    #   e = Polars.col("foo").alias("bar")
    #   e.meta.is_column_selection
    #   # => false
    #
    # @example
    #   e.meta.is_column_selection(allow_aliasing: true)
    #   # => true
    #
    # @example
    #   e = Polars.col("foo") * Polars.col("bar")
    #   e.meta.is_column_selection
    #   # => false
    #
    # @example
    #   e = Polars.cs.starts_with("foo")
    #   e.meta.is_column_selection
    #   # => true
    #
    # @example
    #   e = Polars.cs.starts_with("foo").exclude("foo!")
    #   e.meta.is_column_selection
    #   # => true
    def is_column_selection(allow_aliasing: false)
      _rbexpr.meta_is_column_selection(allow_aliasing)
    end

    # Indicate if this expression is a literal value (optionally aliased).
    #
    # @param allow_aliasing [Boolean]
    #   If false (default), only a bare literal will match.
    #   Set true to also allow for aliased literals.
    #
    # @return [Boolean]
    #
    # @example
    #   e = Polars.lit(123)
    #   e.meta.is_literal
    #   # => true
    #
    # @example
    #   e = Polars.lit(987.654321).alias("foo")
    #   e.meta.is_literal
    #   # => false
    def is_literal(allow_aliasing: false)
      _rbexpr.meta_is_literal(allow_aliasing)
    end

    # Get the column name that this expression would produce.
    #
    # @return [String]
    #
    # @example
    #   e = Polars.col("foo") * Polars.col("bar")
    #   e.meta.output_name
    #   # => "foo"
    #   e_filter = Polars.col("foo").filter(Polars.col("bar") == 13)
    #   e_filter.meta.output_name
    #   # => "foo"
    #   e_sum_over = Polars.sum("foo").over("groups")
    #   e_sum_over.meta.output_name
    #   # => "foo"
    #   e_sum_slice = Polars.sum("foo").slice(Polars.len - 10, Polars.col("bar"))
    #   e_sum_slice.meta.output_name
    #   # => "foo"
    #   Polars.len.meta.output_name
    #   # => "len"
    def output_name
      _rbexpr.meta_output_name
    end

    # Pop the latest expression and return the input(s) of the popped expression.
    #
    # @return [Array]
    #
    # @example
    #   e = Polars.col("foo") + Polars.col("bar")
    #   first = e.meta.pop[0]
    #   _ = first.meta == Polars.col("bar")
    #   # => true
    #   _ = first.meta == Polars.col("foo")
    #   # => false
    def pop(schema: nil)
      _rbexpr.meta_pop(schema).map { |e| Utils.wrap_expr(e) }
    end

    # Get a list with the root column name.
    #
    # @return [Array]
    #
    # @example
    #   e = Polars.col("foo") * Polars.col("bar")
    #   e.meta.root_names
    #   # => ["foo", "bar"]
    #   e_filter = Polars.col("foo").filter(Polars.col("bar") == 13)
    #   e_filter.meta.root_names
    #   # => ["foo", "bar"]
    #   e_sum_over = Polars.sum("foo").over("groups")
    #   e_sum_over.meta.root_names
    #   # => ["foo", "groups"]
    #   e_sum_slice = Polars.sum("foo").slice(Polars.len - 10, Polars.col("bar"))
    #   e_sum_slice.meta.root_names
    #   # => ["foo", "bar"]
    def root_names
      _rbexpr.meta_roots
    end

    # Undo any renaming operation like `alias` or `keep_name`.
    #
    # @return [Expr]
    #
    # @example
    #   e = Polars.col("foo").alias("bar")
    #   _ = e.meta.undo_aliases.meta == Polars.col("foo")
    #   # => true
    #   e = Polars.col("foo").sum.over("bar")
    #   _ = e.name.keep.meta.undo_aliases.meta == e
    #   # => true
    def undo_aliases
      Utils.wrap_expr(_rbexpr.meta_undo_aliases)
    end

    # Try to turn this expression in a selector.
    #
    # Raises if the underlying expressions is not a column or selector.
    #
    # @return [Expr]
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    def as_selector
      Selector._from_rbselector(_rbexpr.into_selector)
    end

    # Serialize this expression to a file or string.
    #
    # @param file [Object]
    #   File path to which the result should be written. If set to `nil`
    #   (default), the output is returned as a string instead.
    #
    # @return [Object]
    #
    # @note
    #   Serialization is not stable across Polars versions: a LazyFrame serialized
    #   in one Polars version may not be deserializable in another Polars version.
    #
    # @example Serialize the expression into a binary representation.
    #   expr = Polars.col("foo").sum.over("bar")
    #   bytes = expr.meta.serialize
    #   Polars::Expr.deserialize(StringIO.new(bytes))
    #   # => col("foo").sum().over([col("bar")])
    def serialize(file = nil)
      raise Todo unless _rbexpr.respond_to?(:serialize_binary)

      serializer = _rbexpr.method(:serialize_binary)

      Utils.serialize_polars_object(serializer, file)
    end

    # Format the expression as a tree.
    #
    # @param return_as_string [Boolean]
    #   If true, return as string rather than printing to stdout.
    #
    # @return [String]
    #
    # @example
    #   e = (Polars.col("foo") * Polars.col("bar")).sum.over(Polars.col("ham")) / 2
    #   e.meta.tree_format(return_as_string: true)
    def tree_format(return_as_string: false, schema: nil)
      s = _rbexpr.meta_tree_format(schema)
      if return_as_string
        s
      else
        puts s
        nil
      end
    end
  end
end
