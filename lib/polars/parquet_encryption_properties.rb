module Polars
  # Properties for writing Parquet files encrypted with Parquet modular encryption.
  class ParquetEncryptionProperties
    # @private
    attr_accessor :_rbencryptionproperties

    # Properties for writing Parquet files encrypted with Parquet modular encryption.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @param footer_key [String]
    #   The key used to encrypt the file footer, or to sign the footer if
    #   `plaintext_footer` is set. If no column keys are specified, all columns
    #   are also encrypted with the footer key.
    # @param column_keys [Object]
    #   Keys used to encrypt columns with column-specific keys, keyed by column
    #   name. If any column keys are specified, only the columns with a key are
    #   encrypted and all other columns are written unencrypted. For nested
    #   columns, the column name is the dot-separated path in the Parquet schema,
    #   e.g. `a.b.c`.
    # @param plaintext_footer [Boolean]
    #   Write the file footer in plaintext rather than encrypting it, so that
    #   readers without the footer key can read the schema and any unencrypted
    #   columns. The footer is still signed with the footer key.
    # @param aad_prefix [String]
    #   The AAD (additional authenticated data) prefix. Readers that provide an
    #   AAD prefix can only read files written with the same prefix, which
    #   protects against a file being swapped for one written with a different
    #   prefix, such as an older version of the file. The same prefix is used for
    #   every file written by a single `write_parquet` or `sink_parquet` call, so
    #   it does not protect against swapping files written together when
    #   partitioning, or files from different calls that used the same prefix.
    # @param store_aad_prefix [Boolean]
    #   Store the AAD prefix in the file. If this is false, readers must provide
    #   the AAD prefix in order to read the file.
    #
    # @example
    #   encryption_properties = Polars::ParquetEncryptionProperties.new(
    #     footer_key: "0123456789012345".b,
    #     column_keys: {"x" => "1234567890123450".b}
    #   )
    def initialize(
      footer_key:,
      column_keys: nil,
      plaintext_footer: false,
      aad_prefix: nil,
      store_aad_prefix: false
    )
      _check_bytes(footer_key, "footer_key")
      column_keys_list = !column_keys.nil? ? column_keys.to_a : []
      column_keys_list.each do |column_name, key|
        _check_bytes(key, "key for column #{column_name.inspect}")
      end
      if !aad_prefix.nil?
        _check_bytes(aad_prefix, "aad_prefix")
      elsif store_aad_prefix
        msg = "store_aad_prefix requires aad_prefix to be set"
        raise ArgumentError, msg
      end

      self._rbencryptionproperties = RbFileEncryptionProperties.new(
        footer_key,
        column_keys_list,
        plaintext_footer,
        aad_prefix,
        store_aad_prefix
      )
    end
  end
end
