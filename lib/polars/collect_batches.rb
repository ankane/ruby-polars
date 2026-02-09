module Polars
  # @private
  class CollectBatches
    include Enumerable

    def initialize(inner)
      @inner = inner
    end

    def each
      return to_enum(:each) unless block_given?

      while (rbdf = @inner.next)
        yield DataFrame._from_rbdf(rbdf)
      end
    end
  end
end
