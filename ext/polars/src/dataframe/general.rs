use std::hash::BuildHasher;

use either::Either;
use magnus::{IntoValue, RArray, Ruby, Value, prelude::*, value::Opaque};
use polars::prelude::*;

use crate::conversion::*;
use crate::prelude::strings_to_pl_smallstr;
use crate::rb_modules::pl_utils;
use crate::ruby::exceptions::RbIndexError;
use crate::ruby::gvl::RubyAttach;
use crate::series::ToRbSeries;
use crate::series::to_series;
use crate::utils::EnterPolarsExt;
use crate::{RbDataFrame, RbLazyFrame, RbPolarsErr, RbResult, RbSeries};

impl RbDataFrame {
    pub fn init(columns: RArray) -> RbResult<Self> {
        let mut cols = Vec::new();
        for i in columns.into_iter() {
            cols.push(<&RbSeries>::try_convert(i)?.series.read().clone().into());
        }
        let df = DataFrame::new_infer_height(cols).map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    pub fn estimated_size(&self) -> usize {
        self.df.read().estimated_size()
    }

    pub fn dtype_strings(&self) -> Vec<String> {
        self.df
            .read()
            .columns()
            .iter()
            .map(|s| format!("{}", s.dtype()))
            .collect()
    }

    pub fn add(rb: &Ruby, self_: &Self, s: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() + &*s.series.read())
    }

    pub fn sub(rb: &Ruby, self_: &Self, s: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() - &*s.series.read())
    }

    pub fn div(rb: &Ruby, self_: &Self, s: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() / &*s.series.read())
    }

    pub fn mul(rb: &Ruby, self_: &Self, s: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() * &*s.series.read())
    }

    pub fn rem(rb: &Ruby, self_: &Self, s: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() % &*s.series.read())
    }

    pub fn add_df(rb: &Ruby, self_: &Self, s: &Self) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() + &*s.df.read())
    }

    pub fn sub_df(rb: &Ruby, self_: &Self, s: &Self) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() - &*s.df.read())
    }

    pub fn div_df(rb: &Ruby, self_: &Self, s: &Self) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() / &*s.df.read())
    }

    pub fn mul_df(rb: &Ruby, self_: &Self, s: &Self) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() * &*s.df.read())
    }

    pub fn rem_df(rb: &Ruby, self_: &Self, s: &Self) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() % &*s.df.read())
    }

    pub fn sample_n(
        rb: &Ruby,
        self_: &Self,
        n: &RbSeries,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<u64>,
    ) -> RbResult<Self> {
        rb.enter_polars_df(|| {
            self_
                .df
                .read()
                .sample_n(&n.series.read(), with_replacement, shuffle, seed)
        })
    }

    pub fn sample_frac(
        rb: &Ruby,
        self_: &Self,
        frac: &RbSeries,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<u64>,
    ) -> RbResult<Self> {
        rb.enter_polars_df(|| {
            self_
                .df
                .read()
                .sample_frac(&frac.series.read(), with_replacement, shuffle, seed)
        })
    }

    pub fn rechunk(rb: &Ruby, self_: &Self) -> RbResult<Self> {
        rb.enter_polars_df(|| {
            let mut df = self_.df.write().clone();
            df.rechunk_mut_par();
            Ok(df)
        })
    }

    pub fn as_str(&self) -> String {
        format!("{}", self.df.read())
    }

    pub fn get_columns(rb: &Ruby, self_: &Self) -> RArray {
        let cols = self_.df.read().columns().to_vec();
        cols.to_rbseries(rb)
    }

    pub fn columns(&self) -> Vec<String> {
        self.df
            .read()
            .get_column_names()
            .iter()
            .map(|v| v.to_string())
            .collect()
    }

    pub fn set_column_names(&self, names: Vec<String>) -> RbResult<()> {
        self.df
            .write()
            .set_column_names(&names)
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn dtypes(ruby: &Ruby, self_: &Self) -> RArray {
        let df = self_.df.read();
        let iter = df
            .columns()
            .iter()
            .map(|s| Wrap(s.dtype().clone()).into_value_with(ruby));
        ruby.ary_from_iter(iter)
    }

    pub fn n_chunks(&self) -> usize {
        self.df.read().first_col_n_chunks()
    }

    pub fn shape(&self) -> (usize, usize) {
        self.df.read().shape()
    }

    pub fn height(&self) -> usize {
        self.df.read().height()
    }

    pub fn width(&self) -> usize {
        self.df.read().width()
    }

    pub fn hstack(rb: &Ruby, self_: &Self, columns: RArray) -> RbResult<Self> {
        let columns = to_series(columns)?;
        let columns = columns.into_iter().map(Into::into).collect::<Vec<_>>();
        rb.enter_polars_df(|| self_.df.read().hstack(&columns))
    }

    pub fn hstack_mut(rb: &Ruby, self_: &Self, columns: RArray) -> RbResult<()> {
        let columns = to_series(columns)?;
        let columns = columns.into_iter().map(Into::into).collect::<Vec<_>>();
        rb.enter_polars(|| self_.df.write().hstack_mut(&columns).map(drop))?;
        Ok(())
    }

    pub fn vstack(rb: &Ruby, self_: &Self, other: &RbDataFrame) -> RbResult<Self> {
        rb.enter_polars_df(|| self_.df.read().vstack(&other.df.read()))
    }

    pub fn vstack_mut(rb: &Ruby, self_: &Self, other: &RbDataFrame) -> RbResult<()> {
        rb.enter_polars(|| {
            // Prevent self-vstack deadlocks.
            let other = other.df.read().clone();
            self_.df.write().vstack_mut(&other)?;
            PolarsResult::Ok(())
        })?;
        Ok(())
    }

    pub fn extend(rb: &Ruby, self_: &Self, other: &RbDataFrame) -> RbResult<()> {
        rb.enter_polars(|| {
            // Prevent self-extend deadlocks.
            let other = other.df.read().clone();
            self_.df.write().extend(&other)
        })?;
        Ok(())
    }

    pub fn drop_in_place(&self, name: String) -> RbResult<RbSeries> {
        let s = self
            .df
            .write()
            .drop_in_place(&name)
            .map_err(RbPolarsErr::from)?;
        let s = s.take_materialized_series();
        Ok(RbSeries::new(s))
    }

    pub fn to_series(&self, index: isize) -> RbResult<RbSeries> {
        let df = &self.df.read();

        let index_adjusted = if index < 0 {
            df.width().checked_sub(index.unsigned_abs())
        } else {
            Some(usize::try_from(index).unwrap())
        };

        let s = index_adjusted.and_then(|i| df.select_at_idx(i));
        match s {
            Some(s) => Ok(RbSeries::new(s.as_materialized_series().clone())),
            None => Err(RbIndexError::new_err(
                polars_err!(oob = index, df.width()).to_string(),
            )),
        }
    }

    pub fn get_column_index(&self, name: String) -> Option<usize> {
        self.df.read().get_column_index(&name)
    }

    pub fn get_column(&self, name: String) -> RbResult<RbSeries> {
        let series = self
            .df
            .read()
            .column(&name)
            .map(|s| RbSeries::new(s.as_materialized_series().clone()))
            .map_err(RbPolarsErr::from)?;
        Ok(series)
    }

    pub fn select(rb: &Ruby, self_: &Self, columns: Vec<String>) -> RbResult<Self> {
        rb.enter_polars_df(|| self_.df.read().select(columns.iter().map(|x| &**x)))
    }

    pub fn gather(rb: &Ruby, self_: &Self, indices: Vec<IdxSize>) -> RbResult<Self> {
        let indices = IdxCa::from_vec("".into(), indices);
        rb.enter_polars_df(|| self_.df.read().take(&indices))
    }

    pub fn gather_with_series(rb: &Ruby, self_: &Self, indices: &RbSeries) -> RbResult<Self> {
        let idx_s = indices.series.read();
        let indices = idx_s.idx().map_err(RbPolarsErr::from)?;
        rb.enter_polars_df(|| self_.df.read().take(indices))
    }

    pub fn replace(&self, column: String, new_col: &RbSeries) -> RbResult<()> {
        self.df
            .write()
            .replace(&column, new_col.clone().series.into_inner().into_column())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn replace_column(&self, index: usize, new_col: &RbSeries) -> RbResult<()> {
        self.df
            .write()
            .replace_column(index, new_col.clone().series.into_inner().into_column())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn insert_column(&self, index: usize, new_col: &RbSeries) -> RbResult<()> {
        self.df
            .write()
            .insert_column(index, new_col.clone().series.into_inner().into_column())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn slice(rb: &Ruby, self_: &Self, offset: i64, length: Option<usize>) -> RbResult<Self> {
        rb.enter_polars_df(|| {
            let df = self_.df.read();
            Ok(df.slice(offset, length.unwrap_or_else(|| df.height())))
        })
    }

    pub fn head(rb: &Ruby, self_: &Self, n: usize) -> RbResult<Self> {
        rb.enter_polars_df(|| Ok(self_.df.read().head(Some(n))))
    }

    pub fn tail(rb: &Ruby, self_: &Self, n: usize) -> RbResult<Self> {
        rb.enter_polars_df(|| Ok(self_.df.read().tail(Some(n))))
    }

    pub fn is_unique(rb: &Ruby, self_: &Self) -> RbResult<RbSeries> {
        rb.enter_polars_series(|| self_.df.read().is_unique())
    }

    pub fn is_duplicated(rb: &Ruby, self_: &Self) -> RbResult<RbSeries> {
        rb.enter_polars_series(|| self_.df.read().is_duplicated())
    }

    pub fn equals(
        rb: &Ruby,
        self_: &Self,
        other: &RbDataFrame,
        null_equal: bool,
    ) -> RbResult<bool> {
        if null_equal {
            rb.enter_polars_ok(|| self_.df.read().equals_missing(&other.df.read()))
        } else {
            rb.enter_polars_ok(|| self_.df.read().equals(&other.df.read()))
        }
    }

    pub fn with_row_index(
        rb: &Ruby,
        self_: &Self,
        name: String,
        offset: Option<IdxSize>,
    ) -> RbResult<Self> {
        rb.enter_polars_df(|| self_.df.read().with_row_index(name.into(), offset))
    }

    pub fn group_by_map_groups(
        rb: &Ruby,
        self_: &Self,
        by: Vec<String>,
        lambda: Value,
        maintain_order: bool,
    ) -> RbResult<Self> {
        rb.enter_polars_df(|| {
            let df = self_.df.read().clone(); // Clone so we can't deadlock on re-entrance from lambda.
            let gb = if maintain_order {
                df.group_by_stable(by.iter().map(|x| &**x))
            } else {
                df.group_by(by.iter().map(|x| &**x))
            }?;

            let lambda = Opaque::from(lambda);
            let function = move |df: DataFrame| {
                Ruby::attach(|rb| {
                    let lambda = rb.get_inner(lambda);

                    let rbpolars = pl_utils(rb);
                    let rbdf = RbDataFrame::new(df);
                    let ruby_df_wrapper: Value =
                        rbpolars.funcall("wrap_df", (rbdf,)).unwrap();

                    // Call the lambda and get a Ruby-side DataFrame wrapper.
                    let result_df_wrapper: Value = match lambda.funcall("call", (ruby_df_wrapper,)) {
                        Ok(rbobj) => rbobj,
                        Err(e) => panic!("UDF failed: {}", e),
                    };
                    let rbdf: &RbDataFrame = result_df_wrapper.funcall("_df", ()).expect(
                        "Could not get DataFrame attribute '_df'. Make sure that you return a DataFrame object.",
                    );

                    Ok(rbdf.clone().df.into_inner())
                })
            };

            gb.apply(function)
        })
    }

    pub fn clone(&self) -> Self {
        Clone::clone(self)
    }

    pub fn unpivot(
        rb: &Ruby,
        self_: &Self,
        on: Option<Vec<String>>,
        index: Vec<String>,
        value_name: Option<String>,
        variable_name: Option<String>,
    ) -> RbResult<Self> {
        use polars_ops::unpivot::UnpivotDF;
        let args = UnpivotArgsIR::new(
            self_.df.read().get_column_names_owned(),
            on.map(strings_to_pl_smallstr),
            strings_to_pl_smallstr(index),
            value_name.map(|s| s.into()),
            variable_name.map(|s| s.into()),
        );

        rb.enter_polars_df(|| self_.df.read().unpivot2(args))
    }

    pub fn partition_by(
        rb: &Ruby,
        self_: &Self,
        by: Vec<String>,
        maintain_order: bool,
        include_key: bool,
    ) -> RbResult<RArray> {
        let out = if maintain_order {
            self_.df.read().partition_by_stable(by, include_key)
        } else {
            self_.df.read().partition_by(by, include_key)
        }
        .map_err(RbPolarsErr::from)?;
        Ok(rb.ary_from_iter(out.into_iter().map(RbDataFrame::new)))
    }

    pub fn lazy(&self) -> RbLazyFrame {
        self.df.read().clone().lazy().into()
    }

    pub fn to_dummies(
        rb: &Ruby,
        self_: &Self,
        columns: Option<Vec<String>>,
        separator: Option<String>,
        drop_first: bool,
        drop_nulls: bool,
    ) -> RbResult<Self> {
        rb.enter_polars_df(|| match columns {
            Some(cols) => self_.df.read().columns_to_dummies(
                cols.iter().map(|x| x as &str).collect(),
                separator.as_deref(),
                drop_first,
                drop_nulls,
            ),
            None => self_
                .df
                .read()
                .to_dummies(separator.as_deref(), drop_first, drop_nulls),
        })
    }

    pub fn null_count(rb: &Ruby, self_: &Self) -> RbResult<Self> {
        rb.enter_polars_df(|| Ok(self_.df.read().null_count()))
    }

    pub fn shrink_to_fit(rb: &Ruby, self_: &Self) -> RbResult<()> {
        rb.enter_polars_ok(|| self_.df.write().shrink_to_fit())
    }

    pub fn hash_rows(
        rb: &Ruby,
        self_: &Self,
        k0: u64,
        k1: u64,
        k2: u64,
        k3: u64,
    ) -> RbResult<RbSeries> {
        let seed = PlFixedStateQuality::default().hash_one((k0, k1, k2, k3));
        let hb = PlSeedableRandomStateQuality::seed_from_u64(seed);
        rb.enter_polars_series(|| self_.df.write().hash_rows(Some(hb)))
    }

    pub fn transpose(
        rb: &Ruby,
        self_: &Self,
        keep_names_as: Option<String>,
        column_names: Value,
    ) -> RbResult<Self> {
        let new_col_names = if let Ok(name) = <Vec<String>>::try_convert(column_names) {
            Some(Either::Right(name))
        } else if let Ok(name) = String::try_convert(column_names) {
            Some(Either::Left(name))
        } else {
            None
        };
        rb.enter_polars_df(|| {
            self_
                .df
                .write()
                .transpose(keep_names_as.as_deref(), new_col_names)
        })
    }

    pub fn upsample(
        rb: &Ruby,
        self_: &Self,
        by: Vec<String>,
        index_column: String,
        every: String,
        stable: bool,
    ) -> RbResult<Self> {
        rb.enter_polars_df(|| {
            if stable {
                self_
                    .df
                    .read()
                    .upsample_stable(by, &index_column, Duration::parse(&every))
            } else {
                self_
                    .df
                    .read()
                    .upsample(by, &index_column, Duration::parse(&every))
            }
        })
    }

    pub fn to_struct(rb: &Ruby, self_: &Self, name: String) -> RbResult<RbSeries> {
        rb.enter_polars_series(|| {
            let ca = self_.df.read().clone().into_struct(name.into());
            Ok(ca)
        })
    }

    pub fn clear(rb: &Ruby, self_: &Self) -> RbResult<Self> {
        rb.enter_polars_df(|| Ok(self_.df.read().clear()))
    }
}
