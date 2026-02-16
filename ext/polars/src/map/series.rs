use magnus::{IntoValue, TryConvert};

use super::*;
use crate::Wrap;
use crate::error::RbPolarsErr;
use crate::prelude::ObjectValue;

pub trait ApplyLambdaGeneric {
    fn apply_generic(&self, rb: &Ruby, lambda: Value, skip_nulls: bool) -> RbResult<Series>;

    fn apply_generic_with_dtype(
        &self,
        rb: &Ruby,
        lambda: Value,
        datatype: &DataType,
        skip_nulls: bool,
    ) -> RbResult<Series>;
}

fn call_and_collect_anyvalues<T, I>(
    rb: &Ruby,
    lambda: Value,
    len: usize,
    iter: I,
    skip_nulls: bool,
) -> RbResult<Vec<AnyValue<'static>>>
where
    T: IntoValue,
    I: Iterator<Item = Option<T>>,
{
    let mut avs = Vec::with_capacity(len);
    for opt_val in iter {
        let arg = match opt_val {
            None if skip_nulls => {
                avs.push(AnyValue::Null);
                continue;
            }
            None => rb.qnil().into_value_with(rb),
            Some(val) => val.into_value_with(rb),
        };
        let av: Option<Wrap<AnyValue>> = lambda.funcall("call", (arg,))?;
        avs.push(av.map(|w| w.0).unwrap_or(AnyValue::Null));
    }
    Ok(avs)
}

impl ApplyLambdaGeneric for BooleanChunked {
    fn apply_generic(&self, rb: &Ruby, lambda: Value, skip_nulls: bool) -> RbResult<Series> {
        let avs = call_and_collect_anyvalues(rb, lambda, self.len(), self.into_iter(), skip_nulls)?;
        Ok(Series::from_any_values(self.name().clone(), &avs, true).map_err(RbPolarsErr::from)?)
    }

    fn apply_generic_with_dtype(
        &self,
        rb: &Ruby,
        lambda: Value,
        datatype: &DataType,
        skip_nulls: bool,
    ) -> RbResult<Series> {
        let avs = call_and_collect_anyvalues(rb, lambda, self.len(), self.into_iter(), skip_nulls)?;
        Ok(
            Series::from_any_values_and_dtype(self.name().clone(), &avs, datatype, true)
                .map_err(RbPolarsErr::from)?,
        )
    }
}

impl<T> ApplyLambdaGeneric for ChunkedArray<T>
where
    T: RbPolarsNumericType,
    T::Native: IntoValue + TryConvert,
{
    fn apply_generic(&self, rb: &Ruby, lambda: Value, skip_nulls: bool) -> RbResult<Series> {
        let avs = call_and_collect_anyvalues(rb, lambda, self.len(), self.into_iter(), skip_nulls)?;
        Ok(Series::from_any_values(self.name().clone(), &avs, true).map_err(RbPolarsErr::from)?)
    }

    fn apply_generic_with_dtype(
        &self,
        rb: &Ruby,
        lambda: Value,
        datatype: &DataType,
        skip_nulls: bool,
    ) -> RbResult<Series> {
        let avs = call_and_collect_anyvalues(rb, lambda, self.len(), self.into_iter(), skip_nulls)?;
        Ok(
            Series::from_any_values_and_dtype(self.name().clone(), &avs, datatype, true)
                .map_err(RbPolarsErr::from)?,
        )
    }
}

impl ApplyLambdaGeneric for StringChunked {
    fn apply_generic(&self, rb: &Ruby, lambda: Value, skip_nulls: bool) -> RbResult<Series> {
        let avs = call_and_collect_anyvalues(rb, lambda, self.len(), self.into_iter(), skip_nulls)?;
        Ok(Series::from_any_values(self.name().clone(), &avs, true).map_err(RbPolarsErr::from)?)
    }

    fn apply_generic_with_dtype(
        &self,
        rb: &Ruby,
        lambda: Value,
        datatype: &DataType,
        skip_nulls: bool,
    ) -> RbResult<Series> {
        let avs = call_and_collect_anyvalues(rb, lambda, self.len(), self.into_iter(), skip_nulls)?;
        Ok(
            Series::from_any_values_and_dtype(self.name().clone(), &avs, datatype, true)
                .map_err(RbPolarsErr::from)?,
        )
    }
}

impl ApplyLambdaGeneric for ListChunked {
    fn apply_generic(&self, rb: &Ruby, lambda: Value, skip_nulls: bool) -> RbResult<Series> {
        let it = self.into_iter().map(|opt_s| opt_s.map(Wrap));
        let avs = call_and_collect_anyvalues(rb, lambda, self.len(), it, skip_nulls)?;
        Ok(Series::from_any_values(self.name().clone(), &avs, true).map_err(RbPolarsErr::from)?)
    }

    fn apply_generic_with_dtype(
        &self,
        rb: &Ruby,
        lambda: Value,
        datatype: &DataType,
        skip_nulls: bool,
    ) -> RbResult<Series> {
        let it = self.into_iter().map(|opt_s| opt_s.map(Wrap));
        let avs = call_and_collect_anyvalues(rb, lambda, self.len(), it, skip_nulls)?;
        Ok(
            Series::from_any_values_and_dtype(self.name().clone(), &avs, datatype, true)
                .map_err(RbPolarsErr::from)?,
        )
    }
}

impl ApplyLambdaGeneric for ArrayChunked {
    fn apply_generic(&self, rb: &Ruby, lambda: Value, skip_nulls: bool) -> RbResult<Series> {
        let it = self.into_iter().map(|opt_s| Some(RbSeries::new(opt_s?)));
        let avs = call_and_collect_anyvalues(rb, lambda, self.len(), it, skip_nulls)?;
        Ok(Series::from_any_values(self.name().clone(), &avs, true).map_err(RbPolarsErr::from)?)
    }

    fn apply_generic_with_dtype(
        &self,
        rb: &Ruby,
        lambda: Value,
        datatype: &DataType,
        skip_nulls: bool,
    ) -> RbResult<Series> {
        let it = self.into_iter().map(|opt_s| Some(RbSeries::new(opt_s?)));
        let avs = call_and_collect_anyvalues(rb, lambda, self.len(), it, skip_nulls)?;
        Ok(
            Series::from_any_values_and_dtype(self.name().clone(), &avs, datatype, true)
                .map_err(RbPolarsErr::from)?,
        )
    }
}

impl ApplyLambdaGeneric for ObjectChunked<ObjectValue> {
    fn apply_generic(&self, rb: &Ruby, lambda: Value, skip_nulls: bool) -> RbResult<Series> {
        // TODO improve into_iter
        let avs = call_and_collect_anyvalues(
            rb,
            lambda,
            self.len(),
            self.into_iter().map(|v| v.cloned()),
            skip_nulls,
        )?;
        Ok(Series::from_any_values(self.name().clone(), &avs, true).map_err(RbPolarsErr::from)?)
    }

    fn apply_generic_with_dtype(
        &self,
        rb: &Ruby,
        lambda: Value,
        datatype: &DataType,
        skip_nulls: bool,
    ) -> RbResult<Series> {
        // TODO improve into_iter
        let avs = call_and_collect_anyvalues(
            rb,
            lambda,
            self.len(),
            self.into_iter().map(|v| v.cloned()),
            skip_nulls,
        )?;
        Ok(
            Series::from_any_values_and_dtype(self.name().clone(), &avs, datatype, true)
                .map_err(RbPolarsErr::from)?,
        )
    }
}

impl ApplyLambdaGeneric for StructChunked {
    fn apply_generic(&self, rb: &Ruby, lambda: Value, skip_nulls: bool) -> RbResult<Series> {
        let it = (0..self.len())
            .map(|i| unsafe { self.get_any_value_unchecked(i).null_to_none().map(Wrap) });
        let avs = call_and_collect_anyvalues(rb, lambda, self.len(), it, skip_nulls)?;
        Ok(Series::from_any_values(self.name().clone(), &avs, true).map_err(RbPolarsErr::from)?)
    }

    fn apply_generic_with_dtype(
        &self,
        rb: &Ruby,
        lambda: Value,
        datatype: &DataType,
        skip_nulls: bool,
    ) -> RbResult<Series> {
        let it = (0..self.len())
            .map(|i| unsafe { self.get_any_value_unchecked(i).null_to_none().map(Wrap) });
        let avs = call_and_collect_anyvalues(rb, lambda, self.len(), it, skip_nulls)?;
        Ok(
            Series::from_any_values_and_dtype(self.name().clone(), &avs, datatype, true)
                .map_err(RbPolarsErr::from)?,
        )
    }
}

impl ApplyLambdaGeneric for BinaryChunked {
    fn apply_generic(&self, rb: &Ruby, lambda: Value, skip_nulls: bool) -> RbResult<Series> {
        let avs = call_and_collect_anyvalues(
            rb,
            lambda,
            self.len(),
            self.into_iter().map(|v| v.map(|v2| rb.str_from_slice(v2))),
            skip_nulls,
        )?;
        Ok(Series::from_any_values(self.name().clone(), &avs, true).map_err(RbPolarsErr::from)?)
    }

    fn apply_generic_with_dtype(
        &self,
        rb: &Ruby,
        lambda: Value,
        datatype: &DataType,
        skip_nulls: bool,
    ) -> RbResult<Series> {
        let avs = call_and_collect_anyvalues(
            rb,
            lambda,
            self.len(),
            self.into_iter().map(|v| v.map(|v2| rb.str_from_slice(v2))),
            skip_nulls,
        )?;
        Ok(
            Series::from_any_values_and_dtype(self.name().clone(), &avs, datatype, true)
                .map_err(RbPolarsErr::from)?,
        )
    }
}

impl<L, P> ApplyLambdaGeneric for Logical<L, P>
where
    L: PolarsDataType,
    P: PolarsDataType,
    Logical<L, P>: LogicalType,
{
    fn apply_generic(&self, rb: &Ruby, lambda: Value, skip_nulls: bool) -> RbResult<Series> {
        let it = (0..self.len())
            .map(|i| unsafe { self.get_any_value_unchecked(i).null_to_none().map(Wrap) });
        let avs = call_and_collect_anyvalues(rb, lambda, self.len(), it, skip_nulls)?;
        Ok(Series::from_any_values(self.name().clone(), &avs, true).map_err(RbPolarsErr::from)?)
    }

    fn apply_generic_with_dtype(
        &self,
        rb: &Ruby,
        lambda: Value,
        datatype: &DataType,
        skip_nulls: bool,
    ) -> RbResult<Series> {
        let it = (0..self.len())
            .map(|i| unsafe { self.get_any_value_unchecked(i).null_to_none().map(Wrap) });
        let avs = call_and_collect_anyvalues(rb, lambda, self.len(), it, skip_nulls)?;
        Ok(
            Series::from_any_values_and_dtype(self.name().clone(), &avs, datatype, true)
                .map_err(RbPolarsErr::from)?,
        )
    }
}

impl ApplyLambdaGeneric for NullChunked {
    fn apply_generic(&self, rb: &Ruby, lambda: Value, skip_nulls: bool) -> RbResult<Series> {
        let it = (0..self.len()).map(|_| None::<Wrap<AnyValue<'static>>>);
        let avs = call_and_collect_anyvalues(rb, lambda, self.len(), it, skip_nulls)?;
        Ok(Series::from_any_values(self.name().clone(), &avs, true).map_err(RbPolarsErr::from)?)
    }

    fn apply_generic_with_dtype(
        &self,
        rb: &Ruby,
        lambda: Value,
        datatype: &DataType,
        skip_nulls: bool,
    ) -> RbResult<Series> {
        let it = (0..self.len()).map(|_| None::<Wrap<AnyValue<'static>>>);
        let avs = call_and_collect_anyvalues(rb, lambda, self.len(), it, skip_nulls)?;
        Ok(
            Series::from_any_values_and_dtype(self.name().clone(), &avs, datatype, true)
                .map_err(RbPolarsErr::from)?,
        )
    }
}

impl ApplyLambdaGeneric for ExtensionChunked {
    fn apply_generic(&self, _rb: &Ruby, _lambda: Value, _skip_nulls: bool) -> RbResult<Series> {
        unreachable!()
    }

    fn apply_generic_with_dtype(
        &self,
        _rb: &Ruby,
        _lambda: Value,
        _datatype: &DataType,
        _skip_nulls: bool,
    ) -> RbResult<Series> {
        unreachable!()
    }
}
