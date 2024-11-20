use magnus::prelude::*;
use magnus::Value;
use polars::export::arrow::array::Array;
use polars::export::arrow::ffi::{ArrowArrayStream, ArrowArrayStreamReader};
use polars::prelude::*;

use super::RbSeries;

use crate::exceptions::RbValueError;
use crate::RbResult;

/// Import `arrow_c_stream` across Ruby boundary.
fn call_arrow_c_stream(ob: Value) -> RbResult<Value> {
    let capsule = ob.funcall("arrow_c_stream", ())?;
    Ok(capsule)
}

pub(crate) fn import_stream_rbcapsule(capsule: Value) -> RbResult<RbSeries> {
    let ptr: usize = capsule.funcall("to_i", ())?;
    let capsule_pointer = ptr as *mut ArrowArrayStream;

    // # Safety
    // capsule holds a valid C ArrowArrayStream pointer, as defined by the Arrow PyCapsule
    // Interface
    let mut stream = unsafe {
        // Takes ownership of the pointed to ArrowArrayStream
        // This acts to move the data out of the capsule pointer, setting the release callback to NULL
        let stream_ptr = Box::new(std::ptr::replace(
            capsule_pointer as _,
            ArrowArrayStream::empty(),
        ));
        ArrowArrayStreamReader::try_new(stream_ptr)
            .map_err(|err| RbValueError::new_err(err.to_string()))?
    };

    let mut produced_arrays: Vec<Box<dyn Array>> = vec![];
    while let Some(array) = unsafe { stream.next() } {
        produced_arrays.push(array.unwrap());
    }

    // Series::try_from fails for an empty vec of chunks
    let s = if produced_arrays.is_empty() {
        let polars_dt = DataType::from_arrow(stream.field().dtype(), false);
        Series::new_empty(stream.field().name.clone(), &polars_dt)
    } else {
        Series::try_from((stream.field(), produced_arrays)).unwrap()
    };
    Ok(RbSeries::new(s))
}

impl RbSeries {
    pub fn from_arrow_c_stream(ob: Value) -> RbResult<Self> {
        let capsule = call_arrow_c_stream(ob)?;
        import_stream_rbcapsule(capsule)
    }
}
