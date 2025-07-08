module Polars
  # Options for scanning files.
  class ScanCastOptions
    # Common configuration for scanning files.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @param integer_cast ['upcast', 'forbid']
    #   Configuration for casting from integer types:
    #
    #   * `upcast`: Allow lossless casting to wider integer types.
    #   * `forbid`: Raises an error if dtypes do not match.
    #
    # @param float_cast ['upcast', 'downcast', 'forbid']
    #   Configuration for casting from float types:
    #
    #   * `upcast`: Allow casting to higher precision float types.
    #   * `downcast`: Allow casting to lower precision float types.
    #   * `forbid`: Raises an error if dtypes do not match.
    #
    # @param datetime_cast ['nanosecond-downcast', 'convert-timezone', 'forbid']
    #   Configuration for casting from datetime types:
    #
    #   * `nanosecond-downcast`: Allow nanosecond precision datetime to be
    #     downcasted to any lower precision. This has a similar effect to
    #     PyArrow's `coerce_int96_timestamp_unit`.
    #   * `convert-timezone`: Allow casting to a different timezone.
    #   * `forbid`: Raises an error if dtypes do not match.
    #
    # @param missing_struct_fields ['insert', 'raise']
    #   Configuration for behavior when struct fields defined in the schema
    #   are missing from the data:
    #
    #   * `insert`: Inserts the missing fields.
    #   * `raise`: Raises an error.
    #
    # @param extra_struct_fields ['ignore', 'raise']
    #   Configuration for behavior when extra struct fields outside of the
    #   defined schema are encountered in the data:
    #
    #   * `ignore`: Silently ignores.
    #   * `raise`: Raises an error.
    def initialize(
      integer_cast: "forbid",
      float_cast: "forbid",
      datetime_cast: "forbid",
      missing_struct_fields: "raise",
      extra_struct_fields: "raise",
      _internal_call: false
    )
      @integer_cast = integer_cast
      @float_cast = float_cast
      @datetime_cast = datetime_cast
      @missing_struct_fields = missing_struct_fields
      @extra_struct_fields = extra_struct_fields
    end

    def self.default
      new(_internal_call: true)
    end
  end
end
