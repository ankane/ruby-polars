use polars_core::prelude::*;

pub(crate) fn normalize_arrow_fields(field: &ArrowField) -> ArrowField {
    // normalize fields with extension dtypes that are otherwise standard dtypes associated
    // with (for us) irrelevant metadata; recreate the field using the inner (standard) dtype
    match field {
        ArrowField {
            dtype: ArrowDataType::Struct(fields),
            ..
        } => {
            let mut normalized = false;
            let normalized_fields: Vec<_> = fields
                .iter()
                .map(|f| {
                    // note: google bigquery column data is returned as a standard arrow dtype, but the
                    // sql type it was loaded from is associated as metadata (resulting in an extension dtype)
                    if let ArrowDataType::Extension(ext_type) = &f.dtype {
                        if ext_type.name.starts_with("google:sqlType:") {
                            normalized = true;
                            return ArrowField::new(
                                f.name.clone(),
                                ext_type.inner.clone(),
                                f.is_nullable,
                            );
                        }
                    }
                    f.clone()
                })
                .collect();

            if normalized {
                ArrowField::new(
                    field.name.clone(),
                    ArrowDataType::Struct(normalized_fields),
                    field.is_nullable,
                )
            } else {
                field.clone()
            }
        },
        _ => field.clone(),
    }
}
