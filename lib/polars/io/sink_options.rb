module Polars
  module IO
    # @private
    class SinkOptions
      attr_reader :mkdir, :maintain_order, :sync_on_close, :storage_options, :credential_provider

      def initialize(
        mkdir: nil,
        maintain_order: nil,
        sync_on_close: nil,
        storage_options: nil,
        credential_provider: nil
      )
        @mkdir = mkdir
        @maintain_order = maintain_order
        @sync_on_close = sync_on_close
        @storage_options = storage_options
        @credential_provider = credential_provider
      end
    end
  end

  SinkOptions = IO::SinkOptions
end
