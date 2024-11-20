use polars::prelude::*;

use crate::conversion::Wrap;
use crate::{RbExpr, RbPolarsErr, RbResult};

impl RbExpr {
    pub fn str_join(&self, delimiter: String, ignore_nulls: bool) -> Self {
        self.inner
            .clone()
            .str()
            .join(&delimiter, ignore_nulls)
            .into()
    }

    pub fn str_to_date(
        &self,
        format: Option<String>,
        strict: bool,
        exact: bool,
        cache: bool,
    ) -> Self {
        let format = format.map(|x| x.into());

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
        time_zone: Option<Wrap<TimeZone>>,
        strict: bool,
        exact: bool,
        cache: bool,
        ambiguous: &Self,
    ) -> Self {
        let format = format.map(|x| x.into());
        let time_zone = time_zone.map(|x| x.0);

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
        let format = format.map(|x| x.into());

        let options = StrptimeOptions {
            format,
            strict,
            cache,
            exact: true,
        };
        self.inner.clone().str().to_time(options).into()
    }

    pub fn str_strip_chars(&self, matches: &Self) -> Self {
        self.inner
            .clone()
            .str()
            .strip_chars(matches.inner.clone())
            .into()
    }

    pub fn str_strip_chars_start(&self, matches: &Self) -> Self {
        self.inner
            .clone()
            .str()
            .strip_chars_start(matches.inner.clone())
            .into()
    }

    pub fn str_strip_chars_end(&self, matches: &Self) -> Self {
        self.inner
            .clone()
            .str()
            .strip_chars_end(matches.inner.clone())
            .into()
    }

    pub fn str_strip_prefix(&self, prefix: &Self) -> Self {
        self.inner
            .clone()
            .str()
            .strip_prefix(prefix.inner.clone())
            .into()
    }

    pub fn str_strip_suffix(&self, suffix: &Self) -> Self {
        self.inner
            .clone()
            .str()
            .strip_suffix(suffix.inner.clone())
            .into()
    }

    pub fn str_slice(&self, start: &Self, length: &Self) -> Self {
        self.inner
            .clone()
            .str()
            .slice(start.inner.clone(), length.inner.clone())
            .into()
    }

    pub fn str_to_uppercase(&self) -> Self {
        self.inner.clone().str().to_uppercase().into()
    }

    pub fn str_to_lowercase(&self) -> Self {
        self.inner.clone().str().to_lowercase().into()
    }

    // requires nightly
    // pub fn str_to_titlecase(&self) -> Self {
    //     self.inner.clone().str().to_titlecase().into()
    // }

    pub fn str_len_bytes(&self) -> Self {
        self.inner.clone().str().len_bytes().into()
    }

    pub fn str_len_chars(&self) -> Self {
        self.inner.clone().str().len_chars().into()
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

    pub fn str_reverse(&self) -> Self {
        self.inner.clone().str().reverse().into()
    }

    pub fn str_pad_start(&self, length: usize, fillchar: char) -> Self {
        self.clone().inner.str().pad_start(length, fillchar).into()
    }

    pub fn str_pad_end(&self, length: usize, fillchar: char) -> Self {
        self.clone().inner.str().pad_end(length, fillchar).into()
    }

    pub fn str_zfill(&self, length: &Self) -> Self {
        self.clone().inner.str().zfill(length.inner.clone()).into()
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
        self.inner.clone().str().hex_encode().into()
    }

    pub fn str_hex_decode(&self, strict: bool) -> Self {
        self.inner.clone().str().hex_decode(strict).into()
    }

    pub fn str_base64_encode(&self) -> Self {
        self.inner.clone().str().base64_encode().into()
    }

    pub fn str_base64_decode(&self, strict: bool) -> Self {
        self.inner.clone().str().base64_decode(strict).into()
    }

    pub fn str_to_integer(&self, base: &Self, strict: bool) -> Self {
        self.inner
            .clone()
            .str()
            .to_integer(base.inner.clone(), strict)
            .with_fmt("str.to_integer")
            .into()
    }

    pub fn str_json_decode(
        &self,
        dtype: Option<Wrap<DataType>>,
        infer_schema_len: Option<usize>,
    ) -> Self {
        let dtype = dtype.map(|wrap| wrap.0);
        self.inner
            .clone()
            .str()
            .json_decode(dtype, infer_schema_len)
            .into()
    }

    pub fn str_json_path_match(&self, pat: &Self) -> Self {
        self.inner
            .clone()
            .str()
            .json_path_match(pat.inner.clone())
            .into()
    }

    pub fn str_extract(&self, pat: &Self, group_index: usize) -> Self {
        self.inner
            .clone()
            .str()
            .extract(pat.inner.clone(), group_index)
            .into()
    }

    pub fn str_extract_all(&self, pat: &RbExpr) -> Self {
        self.inner
            .clone()
            .str()
            .extract_all(pat.inner.clone())
            .into()
    }

    pub fn str_extract_groups(&self, pat: String) -> RbResult<Self> {
        Ok(self
            .inner
            .clone()
            .str()
            .extract_groups(&pat)
            .map_err(RbPolarsErr::from)?
            .into())
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

    pub fn str_split_exact(&self, by: &Self, n: usize) -> Self {
        self.inner
            .clone()
            .str()
            .split_exact(by.inner.clone(), n)
            .into()
    }

    pub fn str_split_exact_inclusive(&self, by: &Self, n: usize) -> Self {
        self.inner
            .clone()
            .str()
            .split_exact_inclusive(by.inner.clone(), n)
            .into()
    }

    pub fn str_splitn(&self, by: &Self, n: usize) -> Self {
        self.inner.clone().str().splitn(by.inner.clone(), n).into()
    }

    pub fn str_to_decimal(&self, infer_len: usize) -> Self {
        self.inner.clone().str().to_decimal(infer_len).into()
    }

    pub fn str_contains_any(&self, patterns: &RbExpr, ascii_case_insensitive: bool) -> Self {
        self.inner
            .clone()
            .str()
            .contains_any(patterns.inner.clone(), ascii_case_insensitive)
            .into()
    }

    pub fn str_replace_many(
        &self,
        patterns: &RbExpr,
        replace_with: &RbExpr,
        ascii_case_insensitive: bool,
    ) -> Self {
        self.inner
            .clone()
            .str()
            .replace_many(
                patterns.inner.clone(),
                replace_with.inner.clone(),
                ascii_case_insensitive,
            )
            .into()
    }
}
