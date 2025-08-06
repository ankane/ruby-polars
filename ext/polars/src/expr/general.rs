use std::ops::Neg;

use magnus::{RArray, Value};
use polars::lazy::dsl;
use polars::prelude::*;
use polars::series::ops::NullBehavior;
use polars_core::series::IsSorted;

use super::selector::RbSelector;
use crate::conversion::{Wrap, parse_fill_null_strategy};
use crate::map::lazy::map_single;
use crate::rb_exprs_to_exprs;
use crate::{RbExpr, RbPolarsErr, RbResult};

impl RbExpr {
    pub fn add(&self, rhs: &Self) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Plus, rhs.inner.clone()).into())
    }

    pub fn sub(&self, rhs: &Self) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Minus, rhs.inner.clone()).into())
    }

    pub fn mul(&self, rhs: &Self) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Multiply, rhs.inner.clone()).into())
    }

    pub fn truediv(&self, rhs: &Self) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::TrueDivide, rhs.inner.clone()).into())
    }

    pub fn _mod(&self, rhs: &Self) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Modulus, rhs.inner.clone()).into())
    }

    pub fn floordiv(&self, rhs: &Self) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::FloorDivide, rhs.inner.clone()).into())
    }

    pub fn neg(&self) -> RbResult<Self> {
        Ok(self.inner.clone().neg().into())
    }

    pub fn to_str(&self) -> String {
        format!("{:?}", self.inner)
    }

    pub fn eq(&self, other: &Self) -> Self {
        self.inner.clone().eq(other.inner.clone()).into()
    }

    pub fn eq_missing(&self, other: &Self) -> Self {
        self.inner.clone().eq_missing(other.inner.clone()).into()
    }

    pub fn neq(&self, other: &Self) -> Self {
        self.inner.clone().neq(other.inner.clone()).into()
    }

    pub fn neq_missing(&self, other: &Self) -> Self {
        self.inner.clone().neq_missing(other.inner.clone()).into()
    }

    pub fn gt(&self, other: &Self) -> Self {
        self.inner.clone().gt(other.inner.clone()).into()
    }

    pub fn gt_eq(&self, other: &Self) -> Self {
        self.inner.clone().gt_eq(other.inner.clone()).into()
    }

    pub fn lt_eq(&self, other: &Self) -> Self {
        self.inner.clone().lt_eq(other.inner.clone()).into()
    }

    pub fn lt(&self, other: &Self) -> Self {
        self.inner.clone().lt(other.inner.clone()).into()
    }

    pub fn alias(&self, name: String) -> Self {
        self.inner.clone().alias(&name).into()
    }

    pub fn not_(&self) -> Self {
        self.inner.clone().not().into()
    }

    pub fn is_null(&self) -> Self {
        self.inner.clone().is_null().into()
    }

    pub fn is_not_null(&self) -> Self {
        self.inner.clone().is_not_null().into()
    }

    pub fn is_infinite(&self) -> Self {
        self.inner.clone().is_infinite().into()
    }

    pub fn is_finite(&self) -> Self {
        self.inner.clone().is_finite().into()
    }

    pub fn is_nan(&self) -> Self {
        self.inner.clone().is_nan().into()
    }

    pub fn is_not_nan(&self) -> Self {
        self.inner.clone().is_not_nan().into()
    }

    pub fn min(&self) -> Self {
        self.inner.clone().min().into()
    }

    pub fn max(&self) -> Self {
        self.inner.clone().max().into()
    }

    pub fn nan_max(&self) -> Self {
        self.inner.clone().nan_max().into()
    }

    pub fn nan_min(&self) -> Self {
        self.inner.clone().nan_min().into()
    }

    pub fn mean(&self) -> Self {
        self.inner.clone().mean().into()
    }

    pub fn median(&self) -> Self {
        self.inner.clone().median().into()
    }

    pub fn sum(&self) -> Self {
        self.inner.clone().sum().into()
    }

    pub fn n_unique(&self) -> Self {
        self.inner.clone().n_unique().into()
    }

    pub fn arg_unique(&self) -> Self {
        self.inner.clone().arg_unique().into()
    }

    pub fn unique(&self) -> Self {
        self.inner.clone().unique().into()
    }

    pub fn unique_stable(&self) -> Self {
        self.inner.clone().unique_stable().into()
    }

    pub fn first(&self) -> Self {
        self.inner.clone().first().into()
    }

    pub fn last(&self) -> Self {
        self.inner.clone().last().into()
    }

    pub fn implode(&self) -> Self {
        self.inner.clone().implode().into()
    }

    pub fn quantile(&self, quantile: &Self, interpolation: Wrap<QuantileMethod>) -> Self {
        self.inner
            .clone()
            .quantile(quantile.inner.clone(), interpolation.0)
            .into()
    }

    pub fn cut(
        &self,
        breaks: Vec<f64>,
        labels: Option<Vec<String>>,
        left_closed: bool,
        include_breaks: bool,
    ) -> Self {
        self.inner
            .clone()
            .cut(breaks, labels, left_closed, include_breaks)
            .into()
    }

    pub fn qcut(
        &self,
        probs: Vec<f64>,
        labels: Option<Vec<String>>,
        left_closed: bool,
        allow_duplicates: bool,
        include_breaks: bool,
    ) -> Self {
        self.inner
            .clone()
            .qcut(probs, labels, left_closed, allow_duplicates, include_breaks)
            .into()
    }

    pub fn qcut_uniform(
        &self,
        n_bins: usize,
        labels: Option<Vec<String>>,
        left_closed: bool,
        allow_duplicates: bool,
        include_breaks: bool,
    ) -> Self {
        self.inner
            .clone()
            .qcut_uniform(
                n_bins,
                labels,
                left_closed,
                allow_duplicates,
                include_breaks,
            )
            .into()
    }

    pub fn rle(&self) -> Self {
        self.inner.clone().rle().into()
    }

    pub fn rle_id(&self) -> Self {
        self.inner.clone().rle_id().into()
    }

    pub fn agg_groups(&self) -> Self {
        self.inner.clone().agg_groups().into()
    }

    pub fn count(&self) -> Self {
        self.inner.clone().count().into()
    }

    pub fn len(&self) -> Self {
        self.inner.clone().len().into()
    }

    pub fn value_counts(&self, sort: bool, parallel: bool, name: String, normalize: bool) -> Self {
        self.inner
            .clone()
            .value_counts(sort, parallel, name.as_str(), normalize)
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
            .sort(SortOptions {
                descending,
                nulls_last,
                multithreaded: true,
                maintain_order: false,
                limit: None,
            })
            .into()
    }

    pub fn arg_sort(&self, descending: bool, nulls_last: bool) -> Self {
        self.inner.clone().arg_sort(descending, nulls_last).into()
    }

    pub fn top_k(&self, k: &Self) -> Self {
        self.inner.clone().top_k(k.inner.clone()).into()
    }

    pub fn top_k_by(&self, by: RArray, k: &Self, reverse: Vec<bool>) -> RbResult<Self> {
        let by = rb_exprs_to_exprs(by)?;
        Ok(self
            .inner
            .clone()
            .top_k_by(k.inner.clone(), by, reverse)
            .into())
    }

    pub fn bottom_k(&self, k: &Self) -> Self {
        self.inner.clone().bottom_k(k.inner.clone()).into()
    }

    pub fn bottom_k_by(&self, by: RArray, k: &Self, reverse: Vec<bool>) -> RbResult<Self> {
        let by = rb_exprs_to_exprs(by)?;
        Ok(self
            .inner
            .clone()
            .bottom_k_by(k.inner.clone(), by, reverse)
            .into())
    }

    pub fn peak_min(&self) -> Self {
        self.inner.clone().peak_min().into()
    }

    pub fn peak_max(&self) -> Self {
        self.inner.clone().peak_max().into()
    }

    pub fn arg_max(&self) -> Self {
        self.inner.clone().arg_max().into()
    }

    pub fn arg_min(&self) -> Self {
        self.inner.clone().arg_min().into()
    }

    pub fn index_of(&self, element: &Self) -> Self {
        self.inner.clone().index_of(element.inner.clone()).into()
    }

    pub fn search_sorted(
        &self,
        element: &Self,
        side: Wrap<SearchSortedSide>,
        descending: bool,
    ) -> Self {
        self.inner
            .clone()
            .search_sorted(element.inner.clone(), side.0, descending)
            .into()
    }

    pub fn gather(&self, idx: &Self) -> Self {
        self.inner.clone().gather(idx.inner.clone()).into()
    }

    pub fn get(&self, idx: &Self) -> Self {
        self.inner.clone().get(idx.inner.clone()).into()
    }

    pub fn sort_by(
        &self,
        by: RArray,
        descending: Vec<bool>,
        nulls_last: Vec<bool>,
        multithreaded: bool,
        maintain_order: bool,
    ) -> RbResult<Self> {
        let by = rb_exprs_to_exprs(by)?;
        Ok(self
            .inner
            .clone()
            .sort_by(
                by,
                SortMultipleOptions {
                    descending,
                    nulls_last,
                    multithreaded,
                    maintain_order,
                    limit: None,
                },
            )
            .into())
    }

    pub fn shift(&self, n: &Self, fill_value: Option<&Self>) -> Self {
        let expr = self.inner.clone();
        let out = match fill_value {
            Some(v) => expr.shift_and_fill(n.inner.clone(), v.inner.clone()),
            None => expr.shift(n.inner.clone()),
        };
        out.into()
    }

    pub fn fill_null(&self, expr: &Self) -> Self {
        self.inner.clone().fill_null(expr.inner.clone()).into()
    }

    pub fn fill_null_with_strategy(
        &self,
        strategy: String,
        limit: FillNullLimit,
    ) -> RbResult<Self> {
        let strategy = parse_fill_null_strategy(&strategy, limit)?;
        Ok(self.inner.clone().fill_null_with_strategy(strategy).into())
    }

    pub fn fill_nan(&self, expr: &Self) -> Self {
        self.inner.clone().fill_nan(expr.inner.clone()).into()
    }

    pub fn drop_nulls(&self) -> Self {
        self.inner.clone().drop_nulls().into()
    }

    pub fn drop_nans(&self) -> Self {
        self.inner.clone().drop_nans().into()
    }

    pub fn filter(&self, predicate: &Self) -> Self {
        self.inner.clone().filter(predicate.inner.clone()).into()
    }

    pub fn reverse(&self) -> Self {
        self.inner.clone().reverse().into()
    }

    pub fn std(&self, ddof: u8) -> Self {
        self.inner.clone().std(ddof).into()
    }

    pub fn var(&self, ddof: u8) -> Self {
        self.inner.clone().var(ddof).into()
    }

    pub fn is_unique(&self) -> Self {
        self.inner.clone().is_unique().into()
    }

    pub fn is_between(&self, lower: &Self, upper: &Self, closed: Wrap<ClosedInterval>) -> Self {
        self.inner
            .clone()
            .is_between(lower.inner.clone(), upper.inner.clone(), closed.0)
            .into()
    }

    pub fn is_close(&self, other: &Self, abs_tol: f64, rel_tol: f64, nans_equal: bool) -> Self {
        self.inner
            .clone()
            .is_close(other.inner.clone(), abs_tol, rel_tol, nans_equal)
            .into()
    }

    pub fn approx_n_unique(&self) -> Self {
        self.inner.clone().approx_n_unique().into()
    }

    pub fn is_first_distinct(&self) -> Self {
        self.inner.clone().is_first_distinct().into()
    }

    pub fn is_last_distinct(&self) -> Self {
        self.inner.clone().is_last_distinct().into()
    }

    pub fn explode(&self) -> Self {
        self.inner.clone().explode().into()
    }

    pub fn gather_every(&self, n: usize, offset: usize) -> Self {
        self.inner.clone().gather_every(n, offset).into()
    }

    pub fn tail(&self, n: Option<usize>) -> Self {
        self.inner.clone().tail(n).into()
    }

    pub fn head(&self, n: Option<usize>) -> Self {
        self.inner.clone().head(n).into()
    }

    pub fn slice(&self, offset: &Self, length: &Self) -> Self {
        self.inner
            .clone()
            .slice(offset.inner.clone(), length.inner.clone())
            .into()
    }

    pub fn append(&self, other: &Self, upcast: bool) -> Self {
        self.inner
            .clone()
            .append(other.inner.clone(), upcast)
            .into()
    }

    pub fn rechunk(&self) -> Self {
        self.inner
            .clone()
            .map(|s| Ok(Some(s.rechunk())), GetOutput::same_type())
            .into()
    }

    pub fn round(&self, decimals: u32, mode: Wrap<RoundMode>) -> Self {
        self.inner.clone().round(decimals, mode.0).into()
    }

    pub fn round_sig_figs(&self, digits: i32) -> Self {
        self.clone().inner.round_sig_figs(digits).into()
    }

    pub fn floor(&self) -> Self {
        self.inner.clone().floor().into()
    }

    pub fn ceil(&self) -> Self {
        self.inner.clone().ceil().into()
    }

    pub fn clip(&self, min: Option<&Self>, max: Option<&Self>) -> Self {
        let expr = self.inner.clone();
        let out = match (min, max) {
            (Some(min), Some(max)) => expr.clip(min.inner.clone(), max.inner.clone()),
            (Some(min), None) => expr.clip_min(min.inner.clone()),
            (None, Some(max)) => expr.clip_max(max.inner.clone()),
            (None, None) => expr,
        };
        out.into()
    }

    pub fn abs(&self) -> Self {
        self.inner.clone().abs().into()
    }

    pub fn sin(&self) -> Self {
        self.inner.clone().sin().into()
    }

    pub fn cos(&self) -> Self {
        self.inner.clone().cos().into()
    }

    pub fn tan(&self) -> Self {
        self.inner.clone().tan().into()
    }

    pub fn cot(&self) -> Self {
        self.inner.clone().cot().into()
    }

    pub fn arcsin(&self) -> Self {
        self.inner.clone().arcsin().into()
    }

    pub fn arccos(&self) -> Self {
        self.inner.clone().arccos().into()
    }

    pub fn arctan(&self) -> Self {
        self.inner.clone().arctan().into()
    }

    pub fn sinh(&self) -> Self {
        self.inner.clone().sinh().into()
    }

    pub fn cosh(&self) -> Self {
        self.inner.clone().cosh().into()
    }

    pub fn tanh(&self) -> Self {
        self.inner.clone().tanh().into()
    }

    pub fn arcsinh(&self) -> Self {
        self.inner.clone().arcsinh().into()
    }

    pub fn arccosh(&self) -> Self {
        self.inner.clone().arccosh().into()
    }

    pub fn arctanh(&self) -> Self {
        self.inner.clone().arctanh().into()
    }

    pub fn degrees(&self) -> Self {
        self.inner.clone().degrees().into()
    }

    pub fn radians(&self) -> Self {
        self.inner.clone().radians().into()
    }

    pub fn sign(&self) -> Self {
        self.inner.clone().sign().into()
    }

    pub fn is_duplicated(&self) -> Self {
        self.inner.clone().is_duplicated().into()
    }

    pub fn over(&self, partition_by: RArray) -> RbResult<Self> {
        let partition_by = rb_exprs_to_exprs(partition_by)?;
        Ok(self.inner.clone().over(partition_by).into())
    }

    pub fn rolling(
        &self,
        index_column: String,
        period: String,
        offset: String,
        closed: Wrap<ClosedWindow>,
    ) -> RbResult<Self> {
        let options = RollingGroupOptions {
            index_column: index_column.into(),
            period: Duration::try_parse(&period).map_err(RbPolarsErr::from)?,
            offset: Duration::try_parse(&offset).map_err(RbPolarsErr::from)?,
            closed_window: closed.0,
        };

        Ok(self.inner.clone().rolling(options).into())
    }

    pub fn _and(&self, expr: &Self) -> Self {
        self.inner.clone().and(expr.inner.clone()).into()
    }

    pub fn _xor(&self, expr: &Self) -> Self {
        self.inner.clone().xor(expr.inner.clone()).into()
    }

    pub fn _or(&self, expr: &Self) -> Self {
        self.inner.clone().or(expr.inner.clone()).into()
    }

    pub fn is_in(&self, expr: &Self, nulls_equal: bool) -> Self {
        self.inner
            .clone()
            .is_in(expr.inner.clone(), nulls_equal)
            .into()
    }

    pub fn repeat_by(&self, by: &Self) -> Self {
        self.inner.clone().repeat_by(by.inner.clone()).into()
    }

    pub fn pow(&self, exponent: &Self) -> Self {
        self.inner.clone().pow(exponent.inner.clone()).into()
    }

    pub fn sqrt(&self) -> Self {
        self.inner.clone().sqrt().into()
    }

    pub fn cbrt(&self) -> Self {
        self.inner.clone().cbrt().into()
    }

    pub fn cum_sum(&self, reverse: bool) -> Self {
        self.inner.clone().cum_sum(reverse).into()
    }

    pub fn cum_max(&self, reverse: bool) -> Self {
        self.inner.clone().cum_max(reverse).into()
    }

    pub fn cum_min(&self, reverse: bool) -> Self {
        self.inner.clone().cum_min(reverse).into()
    }

    pub fn cum_prod(&self, reverse: bool) -> Self {
        self.inner.clone().cum_prod(reverse).into()
    }

    pub fn product(&self) -> Self {
        self.inner.clone().product().into()
    }

    pub fn shrink_dtype(&self) -> Self {
        self.inner.clone().shrink_dtype().into()
    }

    pub fn map_batches(
        &self,
        lambda: Value,
        output_type: Option<Wrap<DataType>>,
        agg_list: bool,
        is_elementwise: bool,
        returns_scalar: bool,
    ) -> Self {
        map_single(
            self,
            lambda,
            output_type,
            agg_list,
            is_elementwise,
            returns_scalar,
        )
    }

    pub fn dot(&self, other: &Self) -> Self {
        self.inner.clone().dot(other.inner.clone()).into()
    }

    pub fn reinterpret(&self, signed: bool) -> Self {
        self.inner.clone().reinterpret(signed).into()
    }

    pub fn mode(&self) -> Self {
        self.inner.clone().mode().into()
    }

    pub fn interpolate(&self, method: Wrap<InterpolationMethod>) -> Self {
        self.inner.clone().interpolate(method.0).into()
    }

    pub fn interpolate_by(&self, by: &Self) -> Self {
        self.inner.clone().interpolate_by(by.inner.clone()).into()
    }

    pub fn lower_bound(&self) -> Self {
        self.inner.clone().lower_bound().into()
    }

    pub fn upper_bound(&self) -> Self {
        self.inner.clone().upper_bound().into()
    }

    pub fn cumulative_eval(&self, expr: &Self, min_samples: usize) -> Self {
        self.inner
            .clone()
            .cumulative_eval(expr.inner.clone(), min_samples)
            .into()
    }

    pub fn rank(&self, method: Wrap<RankMethod>, reverse: bool, seed: Option<u64>) -> Self {
        let options = RankOptions {
            method: method.0,
            descending: reverse,
        };
        self.inner.clone().rank(options, seed).into()
    }

    pub fn diff(&self, n: &Self, null_behavior: Wrap<NullBehavior>) -> Self {
        self.inner
            .clone()
            .diff(n.inner.clone(), null_behavior.0)
            .into()
    }

    pub fn pct_change(&self, n: &Self) -> Self {
        self.inner.clone().pct_change(n.inner.clone()).into()
    }

    pub fn skew(&self, bias: bool) -> Self {
        self.inner.clone().skew(bias).into()
    }

    pub fn kurtosis(&self, fisher: bool, bias: bool) -> Self {
        self.inner.clone().kurtosis(fisher, bias).into()
    }

    pub fn reshape(&self, dims: Vec<i64>) -> Self {
        self.inner.clone().reshape(&dims).into()
    }

    pub fn cum_count(&self, reverse: bool) -> Self {
        self.inner.clone().cum_count(reverse).into()
    }

    pub fn to_physical(&self) -> Self {
        self.inner.clone().to_physical().into()
    }

    pub fn shuffle(&self, seed: Option<u64>) -> Self {
        self.inner.clone().shuffle(seed).into()
    }

    pub fn sample_n(
        &self,
        n: &Self,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<u64>,
    ) -> Self {
        self.inner
            .clone()
            .sample_n(n.inner.clone(), with_replacement, shuffle, seed)
            .into()
    }

    pub fn sample_frac(
        &self,
        frac: &Self,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<u64>,
    ) -> Self {
        self.inner
            .clone()
            .sample_frac(frac.inner.clone(), with_replacement, shuffle, seed)
            .into()
    }

    pub fn ewm_mean(
        &self,
        alpha: f64,
        adjust: bool,
        min_periods: usize,
        ignore_nulls: bool,
    ) -> Self {
        let options = EWMOptions {
            alpha,
            adjust,
            bias: false,
            min_periods,
            ignore_nulls,
        };
        self.inner.clone().ewm_mean(options).into()
    }

    pub fn ewm_std(
        &self,
        alpha: f64,
        adjust: bool,
        bias: bool,
        min_periods: usize,
        ignore_nulls: bool,
    ) -> Self {
        let options = EWMOptions {
            alpha,
            adjust,
            bias,
            min_periods,
            ignore_nulls,
        };
        self.inner.clone().ewm_std(options).into()
    }

    pub fn ewm_var(
        &self,
        alpha: f64,
        adjust: bool,
        bias: bool,
        min_periods: usize,
        ignore_nulls: bool,
    ) -> Self {
        let options = EWMOptions {
            alpha,
            adjust,
            bias,
            min_periods,
            ignore_nulls,
        };
        self.inner.clone().ewm_var(options).into()
    }

    pub fn extend_constant(&self, value: &Self, n: &Self) -> Self {
        self.inner
            .clone()
            .extend_constant(value.inner.clone(), n.inner.clone())
            .into()
    }

    pub fn any(&self, drop_nulls: bool) -> Self {
        self.inner.clone().any(drop_nulls).into()
    }

    pub fn all(&self, drop_nulls: bool) -> Self {
        self.inner.clone().all(drop_nulls).into()
    }

    pub fn log(&self, base: f64) -> Self {
        self.inner.clone().log(base).into()
    }

    pub fn log1p(&self) -> Self {
        self.inner.clone().log1p().into()
    }

    pub fn exp(&self) -> Self {
        self.inner.clone().exp().into()
    }

    pub fn entropy(&self, base: f64, normalize: bool) -> Self {
        self.inner.clone().entropy(base, normalize).into()
    }

    pub fn hash(&self, seed: u64, seed_1: u64, seed_2: u64, seed_3: u64) -> Self {
        self.inner.clone().hash(seed, seed_1, seed_2, seed_3).into()
    }

    pub fn set_sorted_flag(&self, descending: bool) -> Self {
        let is_sorted = if descending {
            IsSorted::Descending
        } else {
            IsSorted::Ascending
        };
        self.inner.clone().set_sorted_flag(is_sorted).into()
    }

    pub fn replace(&self, old: &Self, new: &Self) -> Self {
        self.inner
            .clone()
            .replace(old.inner.clone(), new.inner.clone())
            .into()
    }

    pub fn replace_strict(
        &self,
        old: &Self,
        new: &Self,
        default: Option<&Self>,
        return_dtype: Option<Wrap<DataType>>,
    ) -> Self {
        self.inner
            .clone()
            .replace_strict(
                old.inner.clone(),
                new.inner.clone(),
                default.map(|e| e.inner.clone()),
                return_dtype.map(|dt| dt.0),
            )
            .into()
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn into_selector(&self) -> RbResult<RbSelector> {
        Ok(self
            .inner
            .clone()
            .into_selector()
            .ok_or_else(
                || polars_err!(InvalidOperation: "expr `{}` is not a selector", &self.inner),
            )
            .map_err(RbPolarsErr::from)?
            .into())
    }

    pub fn new_selector(selector: &RbSelector) -> Self {
        Expr::Selector(selector.inner.clone()).into()
    }
}
