use polars::prelude::*;

use crate::conversion::Wrap;
use crate::RbExpr;

impl RbExpr {
    pub fn str_concat(&self, delimiter: String) -> Self {
        self.inner.clone().str().concat(&delimiter).into()
    }

    pub fn str_to_date(
        &self,
        format: Option<String>,
        strict: bool,
        exact: bool,
        cache: bool,
    ) -> Self {
        let options = StrptimeOptions {
            format,
            strict,
            exact,
            cache,
        };
        self.inner.clone().str().to_date(options).into()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn str_to_datetime(
        &self,
        format: Option<String>,
        time_unit: Option<Wrap<TimeUnit>>,
        time_zone: Option<TimeZone>,
        strict: bool,
        exact: bool,
        cache: bool,
        ambiguous: &Self,
    ) -> Self {
        let options = StrptimeOptions {
            format,
            strict,
            exact,
            cache,
        };
        self.inner
            .clone()
            .str()
            .to_datetime(
                time_unit.map(|tu| tu.0),
                time_zone,
                options,
                ambiguous.inner.clone(),
            )
            .into()
    }

    pub fn str_to_time(&self, format: Option<String>, strict: bool, cache: bool) -> Self {
        let options = StrptimeOptions {
            format,
            strict,
            cache,
            exact: true,
        };
        self.inner.clone().str().to_time(options).into()
    }

    pub fn str_strip_chars(&self, matches: Option<String>) -> Self {
        self.inner.clone().str().strip_chars(matches).into()
    }

    pub fn str_strip_chars_start(&self, matches: Option<String>) -> Self {
        self.inner.clone().str().strip_chars_start(matches).into()
    }

    pub fn str_strip_chars_end(&self, matches: Option<String>) -> Self {
        self.inner.clone().str().strip_chars_end(matches).into()
    }

    pub fn str_strip_prefix(&self, prefix: String) -> Self {
        self.inner.clone().str().strip_prefix(prefix).into()
    }

    pub fn str_strip_suffix(&self, suffix: String) -> Self {
        self.inner.clone().str().strip_suffix(suffix).into()
    }

    pub fn str_slice(&self, start: i64, length: Option<u64>) -> Self {
        let function = move |s: Series| {
            let ca = s.utf8()?;
            Ok(Some(ca.str_slice(start, length)?.into_series()))
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::Utf8))
            .with_fmt("str.slice")
            .into()
    }

    pub fn str_explode(&self) -> Self {
        self.inner.clone().str().explode().into()
    }

    pub fn str_to_uppercase(&self) -> Self {
        self.inner.clone().str().to_uppercase().into()
    }

    pub fn str_to_lowercase(&self) -> Self {
        self.inner.clone().str().to_lowercase().into()
    }

    pub fn str_lengths(&self) -> Self {
        let function = |s: Series| {
            let ca = s.utf8()?;
            Ok(Some(ca.str_lengths().into_series()))
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::UInt32))
            .with_fmt("str.lengths")
            .into()
    }

    pub fn str_n_chars(&self) -> Self {
        let function = |s: Series| {
            let ca = s.utf8()?;
            Ok(Some(ca.str_n_chars().into_series()))
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::UInt32))
            .with_fmt("str.n_chars")
            .into()
    }

    pub fn str_replace_n(&self, pat: &RbExpr, val: &RbExpr, literal: bool, n: i64) -> Self {
        self.inner
            .clone()
            .str()
            .replace_n(pat.inner.clone(), val.inner.clone(), literal, n)
            .into()
    }

    pub fn str_replace_all(&self, pat: &RbExpr, val: &RbExpr, literal: bool) -> Self {
        self.inner
            .clone()
            .str()
            .replace_all(pat.inner.clone(), val.inner.clone(), literal)
            .into()
    }

    pub fn str_zfill(&self, alignment: usize) -> Self {
        self.clone().inner.str().zfill(alignment).into()
    }

    pub fn str_ljust(&self, width: usize, fillchar: char) -> Self {
        self.clone().inner.str().ljust(width, fillchar).into()
    }

    pub fn str_rjust(&self, width: usize, fillchar: char) -> Self {
        self.clone().inner.str().rjust(width, fillchar).into()
    }

    pub fn str_contains(&self, pat: &RbExpr, literal: Option<bool>, strict: bool) -> Self {
        match literal {
            Some(true) => self
                .inner
                .clone()
                .str()
                .contains_literal(pat.inner.clone())
                .into(),
            _ => self
                .inner
                .clone()
                .str()
                .contains(pat.inner.clone(), strict)
                .into(),
        }
    }

    pub fn str_ends_with(&self, sub: &RbExpr) -> Self {
        self.inner.clone().str().ends_with(sub.inner.clone()).into()
    }

    pub fn str_starts_with(&self, sub: &RbExpr) -> Self {
        self.inner
            .clone()
            .str()
            .starts_with(sub.inner.clone())
            .into()
    }

    pub fn str_hex_encode(&self) -> Self {
        self.clone()
            .inner
            .map(
                move |s| s.utf8().map(|s| Some(s.hex_encode().into_series())),
                GetOutput::same_type(),
            )
            .with_fmt("str.hex_encode")
            .into()
    }

    pub fn str_hex_decode(&self, strict: bool) -> Self {
        self.clone()
            .inner
            .map(
                move |s| s.utf8()?.hex_decode(strict).map(|s| Some(s.into_series())),
                GetOutput::same_type(),
            )
            .with_fmt("str.hex_decode")
            .into()
    }

    pub fn str_base64_encode(&self) -> Self {
        self.clone()
            .inner
            .map(
                move |s| s.utf8().map(|s| Some(s.base64_encode().into_series())),
                GetOutput::same_type(),
            )
            .with_fmt("str.base64_encode")
            .into()
    }

    pub fn str_base64_decode(&self, strict: bool) -> Self {
        self.clone()
            .inner
            .map(
                move |s| {
                    s.utf8()?
                        .base64_decode(strict)
                        .map(|s| Some(s.into_series()))
                },
                GetOutput::same_type(),
            )
            .with_fmt("str.base64_decode")
            .into()
    }

    pub fn str_parse_int(&self, radix: u32, strict: bool) -> Self {
        self.inner
            .clone()
            .str()
            .from_radix(radix, strict)
            .with_fmt("str.parse_int")
            .into()
    }

    pub fn str_json_extract(
        &self,
        dtype: Option<Wrap<DataType>>,
        infer_schema_len: Option<usize>,
    ) -> Self {
        let dtype = dtype.map(|wrap| wrap.0);

        let output_type = match dtype.clone() {
            Some(dtype) => GetOutput::from_type(dtype),
            None => GetOutput::from_type(DataType::Unknown),
        };

        let function = move |s: Series| {
            let ca = s.utf8()?;
            match ca.json_extract(dtype.clone(), infer_schema_len) {
                Ok(ca) => Ok(Some(ca.into_series())),
                Err(e) => Err(PolarsError::ComputeError(format!("{e:?}").into())),
            }
        };

        self.clone()
            .inner
            .map(function, output_type)
            .with_fmt("str.json_extract")
            .into()
    }

    pub fn str_json_path_match(&self, pat: String) -> Self {
        let function = move |s: Series| {
            let ca = s.utf8()?;
            match ca.json_path_match(&pat) {
                Ok(ca) => Ok(Some(ca.into_series())),
                Err(e) => Err(PolarsError::ComputeError(format!("{:?}", e).into())),
            }
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::Utf8))
            .with_fmt("str.json_path_match")
            .into()
    }

    pub fn str_extract(&self, pat: String, group_index: usize) -> Self {
        self.inner.clone().str().extract(&pat, group_index).into()
    }

    pub fn str_extract_all(&self, pat: &RbExpr) -> Self {
        self.inner
            .clone()
            .str()
            .extract_all(pat.inner.clone())
            .into()
    }

    pub fn str_count_matches(&self, pat: &Self, literal: bool) -> Self {
        self.inner
            .clone()
            .str()
            .count_matches(pat.inner.clone(), literal)
            .into()
    }

    pub fn str_split(&self, by: &Self) -> Self {
        self.inner.clone().str().split(by.inner.clone()).into()
    }

    pub fn str_split_inclusive(&self, by: &Self) -> Self {
        self.inner
            .clone()
            .str()
            .split_inclusive(by.inner.clone())
            .into()
    }

    pub fn str_split_exact(&self, by: String, n: usize) -> Self {
        self.inner.clone().str().split_exact(&by, n).into()
    }

    pub fn str_split_exact_inclusive(&self, by: String, n: usize) -> Self {
        self.inner
            .clone()
            .str()
            .split_exact_inclusive(&by, n)
            .into()
    }

    pub fn str_splitn(&self, by: String, n: usize) -> Self {
        self.inner.clone().str().splitn(&by, n).into()
    }
}
