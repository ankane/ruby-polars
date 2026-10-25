module Polars
  # Properties for reading Parquet files encrypted with Parquet modular encryption.
  class ParquetDecryptionProperties
    # Properties for reading Parquet files encrypted with Parquet modular encryption.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @param footer_key [String]
    #   The key used to decrypt the file footer. This is also used to decrypt
    #   column data for files where all columns are encrypted with the footer key.
    # @param column_keys [Object]
    #   Keys used to decrypt columns that are encrypted with column-specific keys,
    #   keyed by column name. For nested columns, the column name is the
    #   dot-separated path in the Parquet schema, e.g. `a.b.c`.
    # @param aad_prefix [String]
    #   The AAD (additional authenticated data) prefix. This must be provided if
    #   the file was written with an AAD prefix that is not stored in the file.
    # @param verify_footer_signature [Boolean]
    #   Verify the footer signature of encrypted files with a plaintext footer.
    #
    # @example
    #   decryption_properties = Polars::ParquetDecryptionProperties.new(
    #     footer_key: "0123456789012345".b,
    #     column_keys: {"x" => "1234567890123450".b}
    #   )
    def initialize(
      footer_key:,
      column_keys: nil,
      aad_prefix: nil,
      verify_footer_signature: true
    )
      _check_bytes(footer_key, "footer_key")
      column_keys_list = !column_keys.nil? ? column_keys.to_a : []
      column_keys_list.each do |column_name, key|
        _check_bytes(key, "key for column #{column_name.inspect}")
      end
      if !aad_prefix.nil?
        _check_bytes(aad_prefix, "aad_prefix")
      end

      self._rbdecryptionproperties = RbFileDecryptionProperties.new(
        footer_key,
        column_keys_list,
        aad_prefix,
        verify_footer_signature
      )
    end
  end
end
