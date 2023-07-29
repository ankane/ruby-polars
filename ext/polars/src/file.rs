use magnus::{exception, prelude::*, Error, RString, Value};
use polars::io::mmap::MmapBytesReader;
use std::fs::File;
use std::io::Cursor;
use std::path::PathBuf;

use crate::RbResult;

pub fn get_file_like(f: Value, truncate: bool) -> RbResult<File> {
    let str_slice = PathBuf::try_convert(f)?;
    let f = if truncate {
        File::create(str_slice)
            .map_err(|e| Error::new(exception::runtime_error(), e.to_string()))?
    } else {
        File::open(str_slice).map_err(|e| Error::new(exception::runtime_error(), e.to_string()))?
    };
    Ok(f)
}

pub fn get_mmap_bytes_reader(rb_f: Value) -> RbResult<Box<dyn MmapBytesReader>> {
    if let Ok(bytes) = rb_f.funcall::<_, _, RString>("read", ()) {
        let bytes = unsafe { bytes.as_slice() };
        // TODO avoid copy
        Ok(Box::new(Cursor::new(bytes.to_vec())))
    } else {
        let p = PathBuf::try_convert(rb_f)?;
        let f = File::open(p).map_err(|e| Error::new(exception::runtime_error(), e.to_string()))?;
        Ok(Box::new(f))
    }
}
