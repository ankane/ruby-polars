use magnus::{Error, RString, Value};
use polars::io::mmap::MmapBytesReader;
use std::fs::{File, OpenOptions};
use std::io::Cursor;
use std::path::PathBuf;

use crate::RbResult;

pub fn get_file_like(f: Value, truncate: bool) -> RbResult<File> {
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(truncate)
        .open(f.try_convert::<PathBuf>()?)
        .map_err(|e| Error::runtime_error(e.to_string()))
}

pub fn get_mmap_bytes_reader(rb_f: Value) -> RbResult<Box<dyn MmapBytesReader>> {
    if let Ok(bytes) = rb_f.funcall::<_, _, RString>("read", ()) {
        let bytes = unsafe { bytes.as_slice() };
        // TODO avoid copy
        Ok(Box::new(Cursor::new(bytes.to_vec())))
    } else {
        let p = rb_f.try_convert::<PathBuf>()?;
        let f = File::open(p).map_err(|e| Error::runtime_error(e.to_string()))?;
        Ok(Box::new(f))
    }
}
