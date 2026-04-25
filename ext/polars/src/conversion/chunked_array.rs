use magnus::{IntoValue, RArray, RString, Ruby, TryConvert, Value, prelude::*};
use polars::prelude::*;
use polars_compute::decimal::DecimalFmtBuffer;

use super::datetime::datetime_to_rb_object;
use super::{Wrap, get_rbseq, struct_dict};

use crate::RbResult;
use crate::rb_modules::pl_utils;
use crate::ruby::utils::TryIntoValue;

impl TryConvert for Wrap<StringChunked> {
    fn try_convert(obj: Value) -> RbResult<Self> {
        let (seq, len) = get_rbseq(obj)?;
        let mut builder = StringChunkedBuilder::new(PlSmallStr::EMPTY, len);

        for res in seq.into_iter() {
            let item = res;
            match String::try_convert(item) {
                Ok(val) => builder.append_value(&val),
                Err(_) => builder.append_null(),
            }
        }
        Ok(Wrap(builder.finish()))
    }
}

impl TryConvert for Wrap<BinaryChunked> {
    fn try_convert(obj: Value) -> RbResult<Self> {
        let (seq, len) = get_rbseq(obj)?;
        let mut builder = BinaryChunkedBuilder::new(PlSmallStr::EMPTY, len);

        for res in seq.into_iter() {
            let item = res;
            match RString::try_convert(item) {
                Ok(val) => builder.append_value(unsafe { val.as_slice() }),
                Err(_) => builder.append_null(),
            }
        }
        Ok(Wrap(builder.finish()))
    }
}

impl IntoValue for Wrap<&StringChunked> {
    fn into_value_with(self, ruby: &Ruby) -> Value {
        let iter = self.0.into_iter();
        ruby.ary_from_iter(iter).as_value()
    }
}

impl IntoValue for Wrap<&BinaryChunked> {
    fn into_value_with(self, ruby: &Ruby) -> Value {
        let iter = self
            .0
            .into_iter()
            .map(|opt_bytes| opt_bytes.map(|v| ruby.str_from_slice(v)));
        ruby.ary_from_iter(iter).as_value()
    }
}

impl IntoValue for Wrap<&StructChunked> {
    fn into_value_with(self, ruby: &Ruby) -> Value {
        let s = self.0.clone().into_series();
        // todo! iterate its chunks and flatten.
        // make series::iter() accept a chunk index.
        let s = s.rechunk();
        let iter = s.iter().map(|av| match av {
            AnyValue::Struct(_, _, flds) => struct_dict(ruby, av._iter_struct_av(), flds),
            AnyValue::Null => ruby.qnil().as_value(),
            _ => unreachable!(),
        });

        ruby.ary_from_iter(iter).as_value()
    }
}

impl IntoValue for Wrap<&DurationChunked> {
    fn into_value_with(self, ruby: &Ruby) -> Value {
        let utils = pl_utils(ruby);
        let time_unit = Wrap(self.0.time_unit()).into_value_with(ruby);
        let iter = self.0.physical().into_iter().map(|opt_v| {
            opt_v.map(|v| {
                utils
                    .funcall::<_, _, Value>("_to_ruby_duration", (v, time_unit))
                    .unwrap()
            })
        });
        ruby.ary_from_iter(iter).as_value()
    }
}

impl TryIntoValue for Wrap<&DatetimeChunked> {
    fn try_into_value_with(self, ruby: &Ruby) -> RbResult<Value> {
        let time_zone = self.0.time_zone().as_ref();
        let time_unit = self.0.time_unit();
        let iter = self.0.physical().iter().map(|opt_v| {
            opt_v
                .map(|v| datetime_to_rb_object(v, time_unit, time_zone))
                .transpose()
        });
        ruby.ary_try_from_iter(iter).map(|v| v.as_value())
    }
}

impl TryIntoValue for Wrap<&TimeChunked> {
    fn try_into_value_with(self, ruby: &Ruby) -> RbResult<Value> {
        let utils = pl_utils(ruby);
        let iter = self.0.physical().into_iter().map(|opt_v| {
            opt_v
                .map(|v| utils.funcall::<_, _, Value>("_to_ruby_time", (v,)))
                .transpose()
        });
        ruby.ary_try_from_iter(iter).map(|v| v.as_value())
    }
}

impl TryIntoValue for Wrap<&DateChunked> {
    fn try_into_value_with(self, ruby: &Ruby) -> RbResult<Value> {
        let utils = pl_utils(ruby);
        let iter = self.0.physical().into_iter().map(|opt_v| {
            opt_v
                .map(|v| utils.funcall::<_, _, Value>("_to_ruby_date", (v,)))
                .transpose()
        });
        ruby.ary_try_from_iter(iter).map(|v| v.as_value())
    }
}

impl TryIntoValue for Wrap<&DecimalChunked> {
    fn try_into_value_with(self, ruby: &Ruby) -> RbResult<Value> {
        let iter = decimal_to_rbobject_iter(ruby, self.0)?;
        Ok(iter.as_value())
    }
}

pub(crate) fn decimal_to_rbobject_iter(ruby: &Ruby, ca: &DecimalChunked) -> RbResult<RArray> {
    let utils = pl_utils(ruby);
    let rb_precision = ca.precision().into_value_with(ruby);
    let mut buf = DecimalFmtBuffer::new();
    let iter = ca.physical().iter().map(move |opt_v| {
        opt_v
            .map(|v| {
                let s = buf.format_dec128(v, ca.scale(), false, false);
                utils.funcall::<_, _, Value>("_to_ruby_decimal", (rb_precision, s))
            })
            .transpose()
    });
    ruby.ary_try_from_iter(iter)
}
