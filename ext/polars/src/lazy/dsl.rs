use magnus::{class, RArray, RString, Value};
use polars::chunked_array::ops::SortOptions;
use polars::lazy::dsl;
use polars::lazy::dsl::Operator;
use polars::prelude::*;
use polars::series::ops::NullBehavior;

use crate::conversion::*;
use crate::lazy::apply::*;
use crate::lazy::utils::rb_exprs_to_exprs;
use crate::utils::reinterpret;
use crate::{RbResult, RbSeries};

#[magnus::wrap(class = "Polars::RbExpr")]
#[derive(Clone)]
pub struct RbExpr {
    pub inner: dsl::Expr,
}

impl From<dsl::Expr> for RbExpr {
    fn from(inner: dsl::Expr) -> Self {
        RbExpr { inner }
    }
}

impl RbExpr {
    pub fn add(&self, rhs: &RbExpr) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Plus, rhs.inner.clone()).into())
    }

    pub fn sub(&self, rhs: &RbExpr) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Minus, rhs.inner.clone()).into())
    }

    pub fn mul(&self, rhs: &RbExpr) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Multiply, rhs.inner.clone()).into())
    }

    pub fn truediv(&self, rhs: &RbExpr) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::TrueDivide, rhs.inner.clone()).into())
    }

    pub fn _mod(&self, rhs: &RbExpr) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Modulus, rhs.inner.clone()).into())
    }

    pub fn floordiv(&self, rhs: &RbExpr) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::FloorDivide, rhs.inner.clone()).into())
    }

    pub fn to_str(&self) -> String {
        format!("{:?}", self.inner)
    }

    pub fn eq(&self, other: &RbExpr) -> Self {
        self.clone().inner.eq(other.inner.clone()).into()
    }

    pub fn neq(&self, other: &RbExpr) -> Self {
        self.clone().inner.neq(other.inner.clone()).into()
    }

    pub fn gt(&self, other: &RbExpr) -> Self {
        self.clone().inner.gt(other.inner.clone()).into()
    }

    pub fn gt_eq(&self, other: &RbExpr) -> Self {
        self.clone().inner.gt_eq(other.inner.clone()).into()
    }

    pub fn lt_eq(&self, other: &RbExpr) -> Self {
        self.clone().inner.lt_eq(other.inner.clone()).into()
    }

    pub fn lt(&self, other: &RbExpr) -> Self {
        self.clone().inner.lt(other.inner.clone()).into()
    }

    pub fn alias(&self, name: String) -> Self {
        self.clone().inner.alias(&name).into()
    }

    pub fn is_not(&self) -> Self {
        self.clone().inner.not().into()
    }

    pub fn is_null(&self) -> Self {
        self.clone().inner.is_null().into()
    }

    pub fn is_not_null(&self) -> Self {
        self.clone().inner.is_not_null().into()
    }

    pub fn is_infinite(&self) -> Self {
        self.clone().inner.is_infinite().into()
    }

    pub fn is_finite(&self) -> Self {
        self.clone().inner.is_finite().into()
    }

    pub fn is_nan(&self) -> Self {
        self.clone().inner.is_nan().into()
    }

    pub fn is_not_nan(&self) -> Self {
        self.clone().inner.is_not_nan().into()
    }

    pub fn min(&self) -> Self {
        self.clone().inner.min().into()
    }

    pub fn max(&self) -> Self {
        self.clone().inner.max().into()
    }

    pub fn nan_max(&self) -> Self {
        self.clone().inner.nan_max().into()
    }

    pub fn nan_min(&self) -> Self {
        self.clone().inner.nan_min().into()
    }

    pub fn mean(&self) -> Self {
        self.clone().inner.mean().into()
    }

    pub fn median(&self) -> Self {
        self.clone().inner.median().into()
    }

    pub fn sum(&self) -> Self {
        self.clone().inner.sum().into()
    }

    pub fn n_unique(&self) -> Self {
        self.clone().inner.n_unique().into()
    }

    pub fn arg_unique(&self) -> Self {
        self.clone().inner.arg_unique().into()
    }

    pub fn unique(&self) -> Self {
        self.clone().inner.unique().into()
    }

    pub fn unique_stable(&self) -> Self {
        self.clone().inner.unique_stable().into()
    }

    pub fn first(&self) -> Self {
        self.clone().inner.first().into()
    }
    pub fn last(&self) -> Self {
        self.clone().inner.last().into()
    }

    pub fn list(&self) -> Self {
        self.clone().inner.list().into()
    }

    pub fn quantile(&self, quantile: f64, interpolation: Wrap<QuantileInterpolOptions>) -> Self {
        self.clone()
            .inner
            .quantile(quantile, interpolation.0)
            .into()
    }

    pub fn agg_groups(&self) -> Self {
        self.clone().inner.agg_groups().into()
    }

    pub fn count(&self) -> Self {
        self.clone().inner.count().into()
    }

    pub fn value_counts(&self, multithreaded: bool, sorted: bool) -> Self {
        self.inner
            .clone()
            .value_counts(multithreaded, sorted)
            .into()
    }

    pub fn unique_counts(&self) -> Self {
        self.inner.clone().unique_counts().into()
    }

    pub fn null_count(&self) -> Self {
        self.inner.clone().null_count().into()
    }

    pub fn cast(&self, data_type: Wrap<DataType>, strict: bool) -> RbResult<Self> {
        let dt = data_type.0;
        let expr = if strict {
            self.inner.clone().strict_cast(dt)
        } else {
            self.inner.clone().cast(dt)
        };
        Ok(expr.into())
    }

    pub fn sort_with(&self, descending: bool, nulls_last: bool) -> Self {
        self.clone()
            .inner
            .sort_with(SortOptions {
                descending,
                nulls_last,
            })
            .into()
    }

    pub fn arg_sort(&self, reverse: bool, nulls_last: bool) -> Self {
        self.clone()
            .inner
            .arg_sort(SortOptions {
                descending: reverse,
                nulls_last,
            })
            .into()
    }

    pub fn top_k(&self, k: usize, reverse: bool) -> Self {
        self.inner.clone().top_k(k, reverse).into()
    }

    pub fn arg_max(&self) -> Self {
        self.clone().inner.arg_max().into()
    }

    pub fn arg_min(&self) -> Self {
        self.clone().inner.arg_min().into()
    }

    pub fn search_sorted(&self, element: &RbExpr) -> Self {
        self.inner
            .clone()
            .search_sorted(element.inner.clone())
            .into()
    }

    pub fn take(&self, idx: &RbExpr) -> Self {
        self.clone().inner.take(idx.inner.clone()).into()
    }

    pub fn sort_by(&self, by: RArray, reverse: Vec<bool>) -> RbResult<Self> {
        let by = rb_exprs_to_exprs(by)?;
        Ok(self.clone().inner.sort_by(by, reverse).into())
    }

    pub fn backward_fill(&self, limit: FillNullLimit) -> Self {
        self.clone().inner.backward_fill(limit).into()
    }

    pub fn forward_fill(&self, limit: FillNullLimit) -> Self {
        self.clone().inner.forward_fill(limit).into()
    }

    pub fn shift(&self, periods: i64) -> Self {
        self.clone().inner.shift(periods).into()
    }
    pub fn shift_and_fill(&self, periods: i64, fill_value: &RbExpr) -> Self {
        self.clone()
            .inner
            .shift_and_fill(periods, fill_value.inner.clone())
            .into()
    }

    pub fn fill_null(&self, expr: &RbExpr) -> Self {
        self.clone().inner.fill_null(expr.inner.clone()).into()
    }

    pub fn fill_null_with_strategy(
        &self,
        strategy: String,
        limit: FillNullLimit,
    ) -> RbResult<Self> {
        let strat = parse_fill_null_strategy(&strategy, limit)?;
        Ok(self
            .inner
            .clone()
            .apply(move |s| s.fill_null(strat), GetOutput::same_type())
            .with_fmt("fill_null_with_strategy")
            .into())
    }

    pub fn fill_nan(&self, expr: &RbExpr) -> Self {
        self.inner.clone().fill_nan(expr.inner.clone()).into()
    }

    pub fn drop_nulls(&self) -> Self {
        self.inner.clone().drop_nulls().into()
    }

    pub fn drop_nans(&self) -> Self {
        self.inner.clone().drop_nans().into()
    }

    pub fn filter(&self, predicate: &RbExpr) -> Self {
        self.clone().inner.filter(predicate.inner.clone()).into()
    }

    pub fn reverse(&self) -> Self {
        self.clone().inner.reverse().into()
    }

    pub fn std(&self, ddof: u8) -> Self {
        self.clone().inner.std(ddof).into()
    }

    pub fn var(&self, ddof: u8) -> Self {
        self.clone().inner.var(ddof).into()
    }

    pub fn is_unique(&self) -> Self {
        self.clone().inner.is_unique().into()
    }

    pub fn is_first(&self) -> Self {
        self.clone().inner.is_first().into()
    }

    pub fn explode(&self) -> Self {
        self.clone().inner.explode().into()
    }

    pub fn take_every(&self, n: usize) -> Self {
        self.clone()
            .inner
            .map(move |s: Series| Ok(s.take_every(n)), GetOutput::same_type())
            .with_fmt("take_every")
            .into()
    }

    pub fn tail(&self, n: Option<usize>) -> Self {
        self.clone().inner.tail(n).into()
    }

    pub fn head(&self, n: Option<usize>) -> Self {
        self.clone().inner.head(n).into()
    }

    pub fn slice(&self, offset: &RbExpr, length: &RbExpr) -> Self {
        self.inner
            .clone()
            .slice(offset.inner.clone(), length.inner.clone())
            .into()
    }

    pub fn append(&self, other: &RbExpr, upcast: bool) -> Self {
        self.inner
            .clone()
            .append(other.inner.clone(), upcast)
            .into()
    }

    pub fn rechunk(&self) -> Self {
        self.inner
            .clone()
            .map(|s| Ok(s.rechunk()), GetOutput::same_type())
            .into()
    }

    pub fn round(&self, decimals: u32) -> Self {
        self.clone().inner.round(decimals).into()
    }

    pub fn floor(&self) -> Self {
        self.clone().inner.floor().into()
    }

    pub fn ceil(&self) -> Self {
        self.clone().inner.ceil().into()
    }

    pub fn clip(&self, min: Value, max: Value) -> Self {
        let min = min.try_convert::<Wrap<AnyValue>>().unwrap().0;
        let max = max.try_convert::<Wrap<AnyValue>>().unwrap().0;
        self.clone().inner.clip(min, max).into()
    }

    pub fn clip_min(&self, min: Value) -> Self {
        let min = min.try_convert::<Wrap<AnyValue>>().unwrap().0;
        self.clone().inner.clip_min(min).into()
    }

    pub fn clip_max(&self, max: Value) -> Self {
        let max = max.try_convert::<Wrap<AnyValue>>().unwrap().0;
        self.clone().inner.clip_max(max).into()
    }

    pub fn abs(&self) -> Self {
        self.clone().inner.abs().into()
    }

    pub fn sin(&self) -> Self {
        self.clone().inner.sin().into()
    }

    pub fn cos(&self) -> Self {
        self.clone().inner.cos().into()
    }

    pub fn tan(&self) -> Self {
        self.clone().inner.tan().into()
    }

    pub fn arcsin(&self) -> Self {
        self.clone().inner.arcsin().into()
    }

    pub fn arccos(&self) -> Self {
        self.clone().inner.arccos().into()
    }

    pub fn arctan(&self) -> Self {
        self.clone().inner.arctan().into()
    }

    pub fn sinh(&self) -> Self {
        self.clone().inner.sinh().into()
    }

    pub fn cosh(&self) -> Self {
        self.clone().inner.cosh().into()
    }

    pub fn tanh(&self) -> Self {
        self.clone().inner.tanh().into()
    }

    pub fn arcsinh(&self) -> Self {
        self.clone().inner.arcsinh().into()
    }

    pub fn arccosh(&self) -> Self {
        self.clone().inner.arccosh().into()
    }

    pub fn arctanh(&self) -> Self {
        self.clone().inner.arctanh().into()
    }

    pub fn sign(&self) -> Self {
        self.clone().inner.sign().into()
    }

    pub fn is_duplicated(&self) -> Self {
        self.clone().inner.is_duplicated().into()
    }

    pub fn over(&self, partition_by: RArray) -> RbResult<Self> {
        let partition_by = rb_exprs_to_exprs(partition_by)?;
        Ok(self.clone().inner.over(partition_by).into())
    }

    pub fn _and(&self, expr: &RbExpr) -> Self {
        self.clone().inner.and(expr.inner.clone()).into()
    }

    pub fn _xor(&self, expr: &RbExpr) -> Self {
        self.clone().inner.xor(expr.inner.clone()).into()
    }

    pub fn _or(&self, expr: &RbExpr) -> Self {
        self.clone().inner.or(expr.inner.clone()).into()
    }

    pub fn is_in(&self, expr: &RbExpr) -> Self {
        self.clone().inner.is_in(expr.inner.clone()).into()
    }

    pub fn repeat_by(&self, by: &RbExpr) -> Self {
        self.clone().inner.repeat_by(by.inner.clone()).into()
    }

    pub fn pow(&self, exponent: &RbExpr) -> Self {
        self.clone().inner.pow(exponent.inner.clone()).into()
    }

    pub fn cumsum(&self, reverse: bool) -> Self {
        self.clone().inner.cumsum(reverse).into()
    }

    pub fn cummax(&self, reverse: bool) -> Self {
        self.clone().inner.cummax(reverse).into()
    }

    pub fn cummin(&self, reverse: bool) -> Self {
        self.clone().inner.cummin(reverse).into()
    }

    pub fn cumprod(&self, reverse: bool) -> Self {
        self.clone().inner.cumprod(reverse).into()
    }

    pub fn product(&self) -> Self {
        self.clone().inner.product().into()
    }

    pub fn shrink_dtype(&self) -> Self {
        self.inner.clone().shrink_dtype().into()
    }

    pub fn str_parse_date(&self, fmt: Option<String>, strict: bool, exact: bool) -> Self {
        self.inner
            .clone()
            .str()
            .strptime(StrpTimeOptions {
                date_dtype: DataType::Date,
                fmt,
                strict,
                exact,
            })
            .into()
    }

    pub fn str_parse_datetime(&self, fmt: Option<String>, strict: bool, exact: bool) -> Self {
        let tu = match fmt {
            Some(ref fmt) => {
                if fmt.contains("%.9f")
                    || fmt.contains("%9f")
                    || fmt.contains("%f")
                    || fmt.contains("%.f")
                {
                    TimeUnit::Nanoseconds
                } else if fmt.contains("%.3f") || fmt.contains("%3f") {
                    TimeUnit::Milliseconds
                } else {
                    TimeUnit::Microseconds
                }
            }
            None => TimeUnit::Microseconds,
        };
        self.inner
            .clone()
            .str()
            .strptime(StrpTimeOptions {
                date_dtype: DataType::Datetime(tu, None),
                fmt,
                strict,
                exact,
            })
            .into()
    }

    pub fn str_parse_time(&self, fmt: Option<String>, strict: bool, exact: bool) -> Self {
        self.inner
            .clone()
            .str()
            .strptime(StrpTimeOptions {
                date_dtype: DataType::Time,
                fmt,
                strict,
                exact,
            })
            .into()
    }

    pub fn str_strip(&self, matches: Option<char>) -> Self {
        self.inner.clone().str().strip(matches).into()
    }

    pub fn str_rstrip(&self, matches: Option<char>) -> Self {
        self.inner.clone().str().rstrip(matches).into()
    }

    pub fn str_lstrip(&self, matches: Option<char>) -> Self {
        self.inner.clone().str().lstrip(matches).into()
    }

    pub fn str_slice(&self, start: i64, length: Option<u64>) -> Self {
        let function = move |s: Series| {
            let ca = s.utf8()?;
            Ok(ca.str_slice(start, length)?.into_series())
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::Utf8))
            .with_fmt("str.slice")
            .into()
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
            Ok(ca.str_lengths().into_series())
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
            Ok(ca.str_n_chars().into_series())
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::UInt32))
            .with_fmt("str.n_chars")
            .into()
    }

    pub fn str_replace(&self, pat: &RbExpr, val: &RbExpr, literal: bool) -> Self {
        self.inner
            .clone()
            .str()
            .replace(pat.inner.clone(), val.inner.clone(), literal)
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

    pub fn str_contains(&self, pat: String, literal: Option<bool>) -> Self {
        match literal {
            Some(true) => self.inner.clone().str().contains_literal(pat).into(),
            _ => self.inner.clone().str().contains(pat).into(),
        }
    }

    pub fn str_ends_with(&self, sub: String) -> Self {
        self.inner.clone().str().ends_with(sub).into()
    }

    pub fn str_starts_with(&self, sub: String) -> Self {
        self.inner.clone().str().starts_with(sub).into()
    }

    pub fn str_extract(&self, pat: String, group_index: usize) -> Self {
        self.inner.clone().str().extract(&pat, group_index).into()
    }

    pub fn str_extract_all(&self, pat: String) -> Self {
        self.inner.clone().str().extract_all(&pat).into()
    }

    pub fn count_match(&self, pat: String) -> Self {
        self.inner.clone().str().count_match(&pat).into()
    }

    pub fn strftime(&self, fmt: String) -> Self {
        self.inner.clone().dt().strftime(&fmt).into()
    }

    pub fn str_split(&self, by: String) -> Self {
        self.inner.clone().str().split(&by).into()
    }

    pub fn str_split_inclusive(&self, by: String) -> Self {
        self.inner.clone().str().split_inclusive(&by).into()
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

    pub fn arr_lengths(&self) -> Self {
        self.inner.clone().arr().lengths().into()
    }

    pub fn arr_contains(&self, other: &RbExpr) -> Self {
        self.inner
            .clone()
            .arr()
            .contains(other.inner.clone())
            .into()
    }

    pub fn year(&self) -> Self {
        self.clone().inner.dt().year().into()
    }

    pub fn iso_year(&self) -> Self {
        self.clone().inner.dt().iso_year().into()
    }

    pub fn quarter(&self) -> Self {
        self.clone().inner.dt().quarter().into()
    }

    pub fn month(&self) -> Self {
        self.clone().inner.dt().month().into()
    }

    pub fn week(&self) -> Self {
        self.clone().inner.dt().week().into()
    }

    pub fn weekday(&self) -> Self {
        self.clone().inner.dt().weekday().into()
    }

    pub fn day(&self) -> Self {
        self.clone().inner.dt().day().into()
    }

    pub fn ordinal_day(&self) -> Self {
        self.clone().inner.dt().ordinal_day().into()
    }

    pub fn hour(&self) -> Self {
        self.clone().inner.dt().hour().into()
    }

    pub fn minute(&self) -> Self {
        self.clone().inner.dt().minute().into()
    }

    pub fn second(&self) -> Self {
        self.clone().inner.dt().second().into()
    }

    pub fn millisecond(&self) -> Self {
        self.clone().inner.dt().millisecond().into()
    }

    pub fn microsecond(&self) -> Self {
        self.clone().inner.dt().microsecond().into()
    }

    pub fn nanosecond(&self) -> Self {
        self.clone().inner.dt().nanosecond().into()
    }

    pub fn duration_days(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(s.duration()?.days().into_series()),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn duration_hours(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(s.duration()?.hours().into_series()),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn duration_minutes(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(s.duration()?.minutes().into_series()),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn duration_seconds(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(s.duration()?.seconds().into_series()),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn duration_nanoseconds(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(s.duration()?.nanoseconds().into_series()),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn duration_microseconds(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(s.duration()?.microseconds().into_series()),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn duration_milliseconds(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(s.duration()?.milliseconds().into_series()),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn timestamp(&self, tu: Wrap<TimeUnit>) -> Self {
        self.inner.clone().dt().timestamp(tu.0).into()
    }

    pub fn dt_offset_by(&self, by: String) -> Self {
        let by = Duration::parse(&by);
        self.inner.clone().dt().offset_by(by).into()
    }

    pub fn dt_epoch_seconds(&self) -> Self {
        self.clone()
            .inner
            .map(
                |s| {
                    s.timestamp(TimeUnit::Milliseconds)
                        .map(|ca| (ca / 1000).into_series())
                },
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn dt_with_time_unit(&self, tu: Wrap<TimeUnit>) -> Self {
        self.inner.clone().dt().with_time_unit(tu.0).into()
    }

    pub fn dt_with_time_zone(&self, tz: Option<TimeZone>) -> Self {
        self.inner.clone().dt().with_time_zone(tz).into()
    }

    pub fn dt_cast_time_unit(&self, tu: Wrap<TimeUnit>) -> Self {
        self.inner.clone().dt().cast_time_unit(tu.0).into()
    }

    pub fn dt_cast_time_zone(&self, tz: String) -> Self {
        self.inner.clone().dt().cast_time_zone(tz).into()
    }

    pub fn dt_tz_localize(&self, tz: String) -> Self {
        self.inner.clone().dt().tz_localize(tz).into()
    }

    pub fn dt_truncate(&self, every: String, offset: String) -> Self {
        self.inner.clone().dt().truncate(&every, &offset).into()
    }

    pub fn dt_round(&self, every: String, offset: String) -> Self {
        self.inner.clone().dt().round(&every, &offset).into()
    }

    pub fn reinterpret(&self, signed: bool) -> Self {
        let function = move |s: Series| reinterpret(&s, signed);
        let dt = if signed {
            DataType::Int64
        } else {
            DataType::UInt64
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(dt))
            .into()
    }

    pub fn mode(&self) -> Self {
        self.inner.clone().mode().into()
    }

    pub fn keep_name(&self) -> Self {
        self.inner.clone().keep_name().into()
    }

    pub fn prefix(&self, prefix: String) -> Self {
        self.inner.clone().prefix(&prefix).into()
    }

    pub fn suffix(&self, suffix: String) -> Self {
        self.inner.clone().suffix(&suffix).into()
    }

    pub fn exclude(&self, columns: Vec<String>) -> Self {
        self.inner.clone().exclude(columns).into()
    }

    pub fn interpolate(&self) -> Self {
        self.inner.clone().interpolate().into()
    }

    pub fn rolling_sum(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
        };
        self.inner.clone().rolling_sum(options).into()
    }

    pub fn rolling_min(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
        };
        self.inner.clone().rolling_min(options).into()
    }

    pub fn rolling_max(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
        };
        self.inner.clone().rolling_max(options).into()
    }

    pub fn rolling_mean(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
        };

        self.inner.clone().rolling_mean(options).into()
    }

    pub fn rolling_std(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
        };

        self.inner.clone().rolling_std(options).into()
    }

    pub fn rolling_var(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
        };

        self.inner.clone().rolling_var(options).into()
    }

    pub fn rolling_median(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
        };
        self.inner.clone().rolling_median(options).into()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn rolling_quantile(
        &self,
        quantile: f64,
        interpolation: Wrap<QuantileInterpolOptions>,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
        };

        self.inner
            .clone()
            .rolling_quantile(quantile, interpolation.0, options)
            .into()
    }

    pub fn rolling_skew(&self, window_size: usize, bias: bool) -> Self {
        self.inner
            .clone()
            .rolling_apply_float(window_size, move |ca| {
                ca.clone().into_series().skew(bias).unwrap()
            })
            .into()
    }

    pub fn lower_bound(&self) -> Self {
        self.inner.clone().lower_bound().into()
    }

    pub fn upper_bound(&self) -> Self {
        self.inner.clone().upper_bound().into()
    }

    pub fn lst_max(&self) -> Self {
        self.inner.clone().arr().max().into()
    }

    pub fn lst_min(&self) -> Self {
        self.inner.clone().arr().min().into()
    }

    pub fn lst_sum(&self) -> Self {
        self.inner.clone().arr().sum().with_fmt("arr.sum").into()
    }

    pub fn lst_mean(&self) -> Self {
        self.inner.clone().arr().mean().with_fmt("arr.mean").into()
    }

    pub fn lst_sort(&self, reverse: bool) -> Self {
        self.inner
            .clone()
            .arr()
            .sort(SortOptions {
                descending: reverse,
                ..Default::default()
            })
            .with_fmt("arr.sort")
            .into()
    }

    pub fn lst_reverse(&self) -> Self {
        self.inner
            .clone()
            .arr()
            .reverse()
            .with_fmt("arr.reverse")
            .into()
    }

    pub fn lst_unique(&self) -> Self {
        self.inner
            .clone()
            .arr()
            .unique()
            .with_fmt("arr.unique")
            .into()
    }

    pub fn lst_get(&self, index: &RbExpr) -> Self {
        self.inner.clone().arr().get(index.inner.clone()).into()
    }

    pub fn lst_join(&self, separator: String) -> Self {
        self.inner.clone().arr().join(&separator).into()
    }

    pub fn lst_arg_min(&self) -> Self {
        self.inner.clone().arr().arg_min().into()
    }

    pub fn lst_arg_max(&self) -> Self {
        self.inner.clone().arr().arg_max().into()
    }

    pub fn lst_diff(&self, n: usize, null_behavior: Wrap<NullBehavior>) -> RbResult<Self> {
        Ok(self.inner.clone().arr().diff(n, null_behavior.0).into())
    }

    pub fn lst_shift(&self, periods: i64) -> Self {
        self.inner.clone().arr().shift(periods).into()
    }

    pub fn lst_slice(&self, offset: &RbExpr, length: Option<&RbExpr>) -> Self {
        let length = match length {
            Some(i) => i.inner.clone(),
            None => dsl::lit(i64::MAX),
        };
        self.inner
            .clone()
            .arr()
            .slice(offset.inner.clone(), length)
            .into()
    }

    pub fn lst_eval(&self, expr: &RbExpr, parallel: bool) -> Self {
        self.inner
            .clone()
            .arr()
            .eval(expr.inner.clone(), parallel)
            .into()
    }

    pub fn cumulative_eval(&self, expr: &RbExpr, min_periods: usize, parallel: bool) -> Self {
        self.inner
            .clone()
            .cumulative_eval(expr.inner.clone(), min_periods, parallel)
            .into()
    }

    pub fn rank(&self, method: Wrap<RankMethod>, reverse: bool) -> Self {
        let options = RankOptions {
            method: method.0,
            descending: reverse,
        };
        self.inner.clone().rank(options).into()
    }

    pub fn diff(&self, n: usize, null_behavior: Wrap<NullBehavior>) -> Self {
        self.inner.clone().diff(n, null_behavior.0).into()
    }

    pub fn pct_change(&self, n: usize) -> Self {
        self.inner.clone().pct_change(n).into()
    }

    pub fn skew(&self, bias: bool) -> Self {
        self.inner.clone().skew(bias).into()
    }

    pub fn kurtosis(&self, fisher: bool, bias: bool) -> Self {
        self.inner.clone().kurtosis(fisher, bias).into()
    }

    pub fn str_concat(&self, delimiter: String) -> Self {
        self.inner.clone().str().concat(&delimiter).into()
    }

    pub fn cat_set_ordering(&self, ordering: Wrap<CategoricalOrdering>) -> Self {
        self.inner.clone().cat().set_ordering(ordering.0).into()
    }

    pub fn reshape(&self, dims: Vec<i64>) -> Self {
        self.inner.clone().reshape(&dims).into()
    }

    pub fn cumcount(&self, reverse: bool) -> Self {
        self.inner.clone().cumcount(reverse).into()
    }

    pub fn to_physical(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(s.to_physical_repr().into_owned()),
                GetOutput::map_dtype(|dt| dt.to_physical()),
            )
            .with_fmt("to_physical")
            .into()
    }

    pub fn shuffle(&self, seed: Option<u64>) -> Self {
        self.inner.clone().shuffle(seed).into()
    }

    pub fn sample_n(
        &self,
        n: usize,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<u64>,
    ) -> Self {
        self.inner
            .clone()
            .sample_n(n, with_replacement, shuffle, seed)
            .into()
    }

    pub fn sample_frac(
        &self,
        frac: f64,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<u64>,
    ) -> Self {
        self.inner
            .clone()
            .sample_frac(frac, with_replacement, shuffle, seed)
            .into()
    }

    pub fn ewm_mean(&self, alpha: f64, adjust: bool, min_periods: usize) -> Self {
        let options = EWMOptions {
            alpha,
            adjust,
            bias: false,
            min_periods,
        };
        self.inner.clone().ewm_mean(options).into()
    }

    pub fn ewm_std(&self, alpha: f64, adjust: bool, bias: bool, min_periods: usize) -> Self {
        let options = EWMOptions {
            alpha,
            adjust,
            bias,
            min_periods,
        };
        self.inner.clone().ewm_std(options).into()
    }

    pub fn ewm_var(&self, alpha: f64, adjust: bool, bias: bool, min_periods: usize) -> Self {
        let options = EWMOptions {
            alpha,
            adjust,
            bias,
            min_periods,
        };
        self.inner.clone().ewm_var(options).into()
    }

    pub fn extend_constant(&self, value: Wrap<AnyValue>, n: usize) -> Self {
        let value = Value::from(value);
        self.inner
            .clone()
            .apply(
                move |s| {
                    let value = value.try_convert::<Wrap<AnyValue>>().unwrap().0;
                    s.extend_constant(value, n)
                },
                GetOutput::same_type(),
            )
            .with_fmt("extend")
            .into()
    }

    pub fn any(&self) -> Self {
        self.inner.clone().any().into()
    }

    pub fn all(&self) -> Self {
        self.inner.clone().all().into()
    }

    pub fn struct_field_by_name(&self, name: String) -> Self {
        self.inner.clone().struct_().field_by_name(&name).into()
    }

    pub fn struct_field_by_index(&self, index: i64) -> Self {
        self.inner.clone().struct_().field_by_index(index).into()
    }

    pub fn struct_rename_fields(&self, names: Vec<String>) -> Self {
        self.inner.clone().struct_().rename_fields(names).into()
    }

    pub fn log(&self, base: f64) -> Self {
        self.inner.clone().log(base).into()
    }

    pub fn exp(&self) -> Self {
        self.inner.clone().exp().into()
    }

    pub fn entropy(&self, base: f64, normalize: bool) -> Self {
        self.inner.clone().entropy(base, normalize).into()
    }
}

pub fn col(name: String) -> RbExpr {
    dsl::col(&name).into()
}

pub fn count() -> RbExpr {
    dsl::count().into()
}

pub fn first() -> RbExpr {
    dsl::first().into()
}

pub fn last() -> RbExpr {
    dsl::last().into()
}

pub fn cols(names: Vec<String>) -> RbExpr {
    dsl::cols(names).into()
}

pub fn fold(acc: &RbExpr, lambda: Value, exprs: RArray) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;

    let func = move |a: Series, b: Series| binary_lambda(lambda, a, b);
    Ok(polars::lazy::dsl::fold_exprs(acc.inner.clone(), func, exprs).into())
}

// TODO improve
pub fn lit(value: Value) -> RbResult<RbExpr> {
    if value.is_nil() {
        Ok(dsl::lit(Null {}).into())
    } else if let Ok(series) = value.try_convert::<&RbSeries>() {
        Ok(dsl::lit(series.series.borrow().clone()).into())
    } else if let Some(v) = RString::from_value(value) {
        Ok(dsl::lit(v.try_convert::<String>()?).into())
    } else if value.is_kind_of(class::integer()) {
        match value.try_convert::<i64>() {
            Ok(val) => {
                if val > 0 && val < i32::MAX as i64 || val < 0 && val > i32::MIN as i64 {
                    Ok(dsl::lit(val as i32).into())
                } else {
                    Ok(dsl::lit(val).into())
                }
            }
            _ => {
                let val = value.try_convert::<u64>()?;
                Ok(dsl::lit(val).into())
            }
        }
    } else {
        Ok(dsl::lit(value.try_convert::<f64>()?).into())
    }
}

pub fn arange(low: &RbExpr, high: &RbExpr, step: usize) -> RbExpr {
    polars::lazy::dsl::arange(low.inner.clone(), high.inner.clone(), step).into()
}

#[magnus::wrap(class = "Polars::RbWhen")]
#[derive(Clone)]
pub struct RbWhen {
    pub inner: dsl::When,
}

impl From<dsl::When> for RbWhen {
    fn from(inner: dsl::When) -> Self {
        RbWhen { inner }
    }
}

#[magnus::wrap(class = "Polars::RbWhenThen")]
#[derive(Clone)]
pub struct RbWhenThen {
    pub inner: dsl::WhenThen,
}

impl From<dsl::WhenThen> for RbWhenThen {
    fn from(inner: dsl::WhenThen) -> Self {
        RbWhenThen { inner }
    }
}

impl RbWhen {
    pub fn then(&self, expr: &RbExpr) -> RbWhenThen {
        self.inner.clone().then(expr.inner.clone()).into()
    }
}

impl RbWhenThen {
    pub fn overwise(&self, expr: &RbExpr) -> RbExpr {
        self.inner.clone().otherwise(expr.inner.clone()).into()
    }
}

pub fn when(predicate: &RbExpr) -> RbWhen {
    dsl::when(predicate.inner.clone()).into()
}

pub fn concat_str(s: RArray, sep: String) -> RbResult<RbExpr> {
    let s = rb_exprs_to_exprs(s)?;
    Ok(dsl::concat_str(s, &sep).into())
}

pub fn concat_lst(s: RArray) -> RbResult<RbExpr> {
    let s = rb_exprs_to_exprs(s)?;
    Ok(dsl::concat_lst(s).into())
}
