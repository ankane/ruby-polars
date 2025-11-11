module Polars
  # A placeholder for an in process query.
  #
  # This can be used to do something else while a query is running.
  # The queries can be cancelled. You can peek if the query is finished,
  # or you can await the result.
  class InProcessQuery
    # @private
    attr_accessor :_inner

    def initialize(ipq)
      self._inner = ipq
    end

    # Cancel the query at earliest convenience.
    def cancel
      _inner.cancel
    end

    # Fetch the result.
    #
    # If it is ready, a materialized DataFrame is returned.
    # If it is not ready it will return `nil`.
    def fetch
      if !(out = _inner.fetch).nil?
        Utils.wrap_df(out)
      else
        nil
      end
    end

    # Await the result synchronously.
    def fetch_blocking
      Utils.wrap_df(_inner.fetch_blocking)
    end
  end
end
