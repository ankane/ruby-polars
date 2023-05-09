use magnus::Value;
use polars::prelude::*;
use polars_core::utils::CustomIterTools;

use crate::conversion::get_rbseq;
use crate::{RbPolarsErr, RbResult};

pub fn rb_seq_to_list(name: &str, seq: Value, dtype: &DataType) -> RbResult<Series> {
    let (seq, len) = get_rbseq(seq)?;

    let s = match dtype {
        DataType::Int64 => {
            let mut builder =
                ListPrimitiveChunkedBuilder::<Int64Type>::new(name, len, len * 5, DataType::Int64);
            for sub_seq in seq.each() {
                let sub_seq = sub_seq?;
                let (sub_seq, len) = get_rbseq(sub_seq)?;

                // safety: we know the iterators len
                let iter = unsafe {
                    sub_seq
                        .each()
                        .map(|v| {
                            let v = v.unwrap();
                            if v.is_nil() {
                                None
                            } else {
                                Some(v.try_convert::<i64>().unwrap())
                            }
                        })
                        .trust_my_length(len)
                };
                builder.append_iter(iter)
            }
            builder.finish().into_series()
        }
        DataType::Float64 => {
            let mut builder = ListPrimitiveChunkedBuilder::<Float64Type>::new(
                name,
                len,
                len * 5,
                DataType::Float64,
            );
            for sub_seq in seq.each() {
                let sub_seq = sub_seq?;
                let (sub_seq, len) = get_rbseq(sub_seq)?;
                // safety: we know the iterators len
                let iter = unsafe {
                    sub_seq
                        .each()
                        .map(|v| {
                            let v = v.unwrap();
                            if v.is_nil() {
                                None
                            } else {
                                Some(v.try_convert::<f64>().unwrap())
                            }
                        })
                        .trust_my_length(len)
                };
                builder.append_iter(iter)
            }
            builder.finish().into_series()
        }
        DataType::Boolean => {
            let mut builder = ListBooleanChunkedBuilder::new(name, len, len * 5);
            for sub_seq in seq.each() {
                let sub_seq = sub_seq?;
                let (sub_seq, len) = get_rbseq(sub_seq)?;
                // safety: we know the iterators len
                let iter = unsafe {
                    sub_seq
                        .each()
                        .map(|v| {
                            let v = v.unwrap();
                            if v.is_nil() {
                                None
                            } else {
                                Some(v.try_convert::<bool>().unwrap())
                            }
                        })
                        .trust_my_length(len)
                };
                builder.append_iter(iter)
            }
            builder.finish().into_series()
        }
        DataType::Utf8 => {
            return Err(RbPolarsErr::todo());
        }
        dt => {
            return Err(RbPolarsErr::other(format!(
                "cannot create list array from {:?}",
                dt
            )));
        }
    };

    Ok(s)
}
