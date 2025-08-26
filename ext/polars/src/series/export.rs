use magnus::{IntoValue, Ruby, Value};
use polars_core::prelude::*;

use crate::RbSeries;
use crate::prelude::*;

impl RbSeries {
    /// Convert this Series to a Ruby array.
    /// This operation copies data.
    pub fn to_a(&self) -> Value {
        let series = &self.series.borrow();

        fn to_a_recursive(series: &Series) -> Value {
            let ruby = Ruby::get().unwrap();
            let rblist = match series.dtype() {
                DataType::Boolean => ruby
                    .ary_from_iter(series.bool().unwrap())
                    .into_value_with(&ruby),
                DataType::UInt8 => ruby
                    .ary_from_iter(series.u8().unwrap())
                    .into_value_with(&ruby),
                DataType::UInt16 => ruby
                    .ary_from_iter(series.u16().unwrap())
                    .into_value_with(&ruby),
                DataType::UInt32 => ruby
                    .ary_from_iter(series.u32().unwrap())
                    .into_value_with(&ruby),
                DataType::UInt64 => ruby
                    .ary_from_iter(series.u64().unwrap())
                    .into_value_with(&ruby),
                DataType::Int8 => ruby
                    .ary_from_iter(series.i8().unwrap())
                    .into_value_with(&ruby),
                DataType::Int16 => ruby
                    .ary_from_iter(series.i16().unwrap())
                    .into_value_with(&ruby),
                DataType::Int32 => ruby
                    .ary_from_iter(series.i32().unwrap())
                    .into_value_with(&ruby),
                DataType::Int64 => ruby
                    .ary_from_iter(series.i64().unwrap())
                    .into_value_with(&ruby),
                DataType::Int128 => ruby
                    .ary_from_iter(series.i128().unwrap())
                    .into_value_with(&ruby),
                DataType::Float32 => ruby
                    .ary_from_iter(series.f32().unwrap())
                    .into_value_with(&ruby),
                DataType::Float64 => ruby
                    .ary_from_iter(series.f64().unwrap())
                    .into_value_with(&ruby),
                DataType::Categorical(_, _) | DataType::Enum(_, _) => {
                    with_match_categorical_physical_type!(series.dtype().cat_physical().unwrap(), |$C| {
                        ruby.ary_from_iter(series.cat::<$C>().unwrap().iter_str()).into_value_with(&ruby)
                    })
                }
                DataType::Object(_) => {
                    let v = ruby.ary_new_capa(series.len());
                    for i in 0..series.len() {
                        let obj: Option<&ObjectValue> = series.get_object(i).map(|any| any.into());
                        match obj {
                            Some(val) => v.push(val.to_value()).unwrap(),
                            None => v.push(ruby.qnil()).unwrap(),
                        };
                    }
                    v.into_value_with(&ruby)
                }
                DataType::List(_) => {
                    let v = ruby.ary_new();
                    let ca = series.list().unwrap();
                    for opt_s in ca.amortized_iter() {
                        match opt_s {
                            None => {
                                v.push(ruby.qnil()).unwrap();
                            }
                            Some(s) => {
                                let rblst = to_a_recursive(s.as_ref());
                                v.push(rblst).unwrap();
                            }
                        }
                    }
                    v.into_value_with(&ruby)
                }
                DataType::Array(_, _) => {
                    let v = ruby.ary_new();
                    let ca = series.array().unwrap();
                    for opt_s in ca.amortized_iter() {
                        match opt_s {
                            None => {
                                v.push(ruby.qnil()).unwrap();
                            }
                            Some(s) => {
                                let rblst = to_a_recursive(s.as_ref());
                                v.push(rblst).unwrap();
                            }
                        }
                    }
                    v.into_value_with(&ruby)
                }
                DataType::Date => {
                    let ca = series.date().unwrap();
                    return Wrap(ca).into_value_with(&ruby);
                }
                DataType::Time => {
                    let ca = series.time().unwrap();
                    return Wrap(ca).into_value_with(&ruby);
                }
                DataType::Datetime(_, _) => {
                    let ca = series.datetime().unwrap();
                    return Wrap(ca).into_value_with(&ruby);
                }
                DataType::Decimal(_, _) => {
                    let ca = series.decimal().unwrap();
                    return Wrap(ca).into_value_with(&ruby);
                }
                DataType::String => {
                    let ca = series.str().unwrap();
                    return Wrap(ca).into_value_with(&ruby);
                }
                DataType::Struct(_) => {
                    let ca = series.struct_().unwrap();
                    return Wrap(ca).into_value_with(&ruby);
                }
                DataType::Duration(_) => {
                    let ca = series.duration().unwrap();
                    return Wrap(ca).into_value_with(&ruby);
                }
                DataType::Binary => {
                    let ca = series.binary().unwrap();
                    return Wrap(ca).into_value_with(&ruby);
                }
                DataType::Null => {
                    let null: Option<u8> = None;
                    let n = series.len();
                    let iter = std::iter::repeat_n(null, n);
                    use std::iter::RepeatN;
                    struct NullIter {
                        iter: RepeatN<Option<u8>>,
                        n: usize,
                    }
                    impl Iterator for NullIter {
                        type Item = Option<u8>;

                        fn next(&mut self) -> Option<Self::Item> {
                            self.iter.next()
                        }
                        fn size_hint(&self) -> (usize, Option<usize>) {
                            (self.n, Some(self.n))
                        }
                    }
                    impl ExactSizeIterator for NullIter {}

                    Ruby::get()
                        .unwrap()
                        .ary_from_iter(NullIter { iter, n })
                        .into_value_with(&ruby)
                }
                DataType::Unknown(_) => {
                    panic!("to_a not implemented for unknown")
                }
                DataType::BinaryOffset => {
                    unreachable!()
                }
            };
            rblist.into_value_with(&ruby)
        }

        to_a_recursive(series)
    }
}
