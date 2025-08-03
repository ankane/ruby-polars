use magnus::{IntoValue, RArray, Value, value::qnil};
use polars_core::prelude::*;

use crate::RbSeries;
use crate::prelude::*;

impl RbSeries {
    /// Convert this Series to a Ruby array.
    /// This operation copies data.
    pub fn to_a(&self) -> Value {
        let series = &self.series.borrow();

        fn to_a_recursive(series: &Series) -> Value {
            let rblist = match series.dtype() {
                DataType::Boolean => RArray::from_iter(series.bool().unwrap()).into_value(),
                DataType::UInt8 => RArray::from_iter(series.u8().unwrap()).into_value(),
                DataType::UInt16 => RArray::from_iter(series.u16().unwrap()).into_value(),
                DataType::UInt32 => RArray::from_iter(series.u32().unwrap()).into_value(),
                DataType::UInt64 => RArray::from_iter(series.u64().unwrap()).into_value(),
                DataType::Int8 => RArray::from_iter(series.i8().unwrap()).into_value(),
                DataType::Int16 => RArray::from_iter(series.i16().unwrap()).into_value(),
                DataType::Int32 => RArray::from_iter(series.i32().unwrap()).into_value(),
                DataType::Int64 => RArray::from_iter(series.i64().unwrap()).into_value(),
                DataType::Int128 => todo!(),
                DataType::Float32 => RArray::from_iter(series.f32().unwrap()).into_value(),
                DataType::Float64 => RArray::from_iter(series.f64().unwrap()).into_value(),
                DataType::Categorical(_, _) | DataType::Enum(_, _) => {
                    with_match_categorical_physical_type!(series.dtype().cat_physical().unwrap(), |$C| {
                        RArray::from_iter(series.cat::<$C>().unwrap().iter_str()).into_value()
                    })
                }
                DataType::Object(_) => {
                    let v = RArray::with_capacity(series.len());
                    for i in 0..series.len() {
                        let obj: Option<&ObjectValue> = series.get_object(i).map(|any| any.into());
                        match obj {
                            Some(val) => v.push(val.to_value()).unwrap(),
                            None => v.push(qnil()).unwrap(),
                        };
                    }
                    v.into_value()
                }
                DataType::List(_) => {
                    let v = RArray::new();
                    let ca = series.list().unwrap();
                    for opt_s in ca.amortized_iter() {
                        match opt_s {
                            None => {
                                v.push(qnil()).unwrap();
                            }
                            Some(s) => {
                                let rblst = to_a_recursive(s.as_ref());
                                v.push(rblst).unwrap();
                            }
                        }
                    }
                    v.into_value()
                }
                DataType::Array(_, _) => {
                    let v = RArray::new();
                    let ca = series.array().unwrap();
                    for opt_s in ca.amortized_iter() {
                        match opt_s {
                            None => {
                                v.push(qnil()).unwrap();
                            }
                            Some(s) => {
                                let rblst = to_a_recursive(s.as_ref());
                                v.push(rblst).unwrap();
                            }
                        }
                    }
                    v.into_value()
                }
                DataType::Date => {
                    let ca = series.date().unwrap();
                    return Wrap(ca).into_value();
                }
                DataType::Time => {
                    let ca = series.time().unwrap();
                    return Wrap(ca).into_value();
                }
                DataType::Datetime(_, _) => {
                    let ca = series.datetime().unwrap();
                    return Wrap(ca).into_value();
                }
                DataType::Decimal(_, _) => {
                    let ca = series.decimal().unwrap();
                    return Wrap(ca).into_value();
                }
                DataType::String => {
                    let ca = series.str().unwrap();
                    return Wrap(ca).into_value();
                }
                DataType::Struct(_) => {
                    let ca = series.struct_().unwrap();
                    return Wrap(ca).into_value();
                }
                DataType::Duration(_) => {
                    let ca = series.duration().unwrap();
                    return Wrap(ca).into_value();
                }
                DataType::Binary => {
                    let ca = series.binary().unwrap();
                    return Wrap(ca).into_value();
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

                    RArray::from_iter(NullIter { iter, n }).into_value()
                }
                DataType::Unknown(_) => {
                    panic!("to_a not implemented for unknown")
                }
                DataType::BinaryOffset => {
                    unreachable!()
                }
            };
            rblist.into_value()
        }

        to_a_recursive(series)
    }
}
