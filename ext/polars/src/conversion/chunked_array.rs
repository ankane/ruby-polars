use magnus::{prelude::*, IntoValue, RArray, RString, Ruby, TryConvert, Value};
use polars::prelude::*;

use super::{get_rbseq, struct_dict, Wrap};

use crate::rb_modules::utils;
use crate::RbResult;

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
    fn into_value_with(self, _: &Ruby) -> Value {
        let iter = self.0.into_iter();
        RArray::from_iter(iter).into_value()
    }
}

impl IntoValue for Wrap<&BinaryChunked> {
    fn into_value_with(self, _: &Ruby) -> Value {
        let iter = self
            .0
            .into_iter()
            .map(|opt_bytes| opt_bytes.map(RString::from_slice));
        RArray::from_iter(iter).into_value()
    }
}

impl IntoValue for Wrap<&StructChunked> {
    fn into_value_with(self, ruby: &Ruby) -> Value {
        let s = self.0.clone().into_series();
        // todo! iterate its chunks and flatten.
        // make series::iter() accept a chunk index.
        let s = s.rechunk();
        let iter = s.iter().map(|av| match av {
            AnyValue::Struct(_, _, flds) => struct_dict(av._iter_struct_av(), flds),
            AnyValue::Null => ruby.qnil().as_value(),
            _ => unreachable!(),
        });

        RArray::from_iter(iter).into_value()
    }
}

impl IntoValue for Wrap<&DurationChunked> {
    fn into_value_with(self, _: &Ruby) -> Value {
        let utils = utils();
        let time_unit = Wrap(self.0.time_unit()).into_value();
        let iter = self.0.into_iter().map(|opt_v| {
            opt_v.map(|v| {
                utils
                    .funcall::<_, _, Value>("_to_ruby_duration", (v, time_unit))
                    .unwrap()
            })
        });
        RArray::from_iter(iter).into_value()
    }
}

impl IntoValue for Wrap<&DatetimeChunked> {
    fn into_value_with(self, _: &Ruby) -> Value {
        let utils = utils();
        let time_unit = Wrap(self.0.time_unit()).into_value();
        let time_zone = self.0.time_zone().as_deref().map(|v| v.into_value());
        let iter = self.0.into_iter().map(|opt_v| {
            opt_v.map(|v| {
                utils
                    .funcall::<_, _, Value>("_to_ruby_datetime", (v, time_unit, time_zone))
                    .unwrap()
            })
        });
        RArray::from_iter(iter).into_value()
    }
}

impl IntoValue for Wrap<&TimeChunked> {
    fn into_value_with(self, _: &Ruby) -> Value {
        let utils = utils();
        let iter = self.0.into_iter().map(|opt_v| {
            opt_v.map(|v| utils.funcall::<_, _, Value>("_to_ruby_time", (v,)).unwrap())
        });
        RArray::from_iter(iter).into_value()
    }
}

impl IntoValue for Wrap<&DateChunked> {
    fn into_value_with(self, _: &Ruby) -> Value {
        let utils = utils();
        let iter = self.0.into_iter().map(|opt_v| {
            opt_v.map(|v| utils.funcall::<_, _, Value>("_to_ruby_date", (v,)).unwrap())
        });
        RArray::from_iter(iter).into_value()
    }
}

impl IntoValue for Wrap<&DecimalChunked> {
    fn into_value_with(self, _: &Ruby) -> Value {
        let utils = utils();
        let rb_scale = (-(self.0.scale() as i32)).into_value();
        let iter = self.0.into_iter().map(|opt_v| {
            opt_v.map(|v| {
                utils
                    .funcall::<_, _, Value>("_to_ruby_decimal", (v.to_string(), rb_scale))
                    .unwrap()
            })
        });
        RArray::from_iter(iter).into_value()
    }
}
