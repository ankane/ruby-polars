use std::fs::File;
use std::io;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use magnus::{Error, RString, Ruby, Value, exception, prelude::*, value::Opaque};
use polars::io::cloud::CloudOptions;
use polars::io::mmap::MmapBytesReader;
use polars::prelude::file::DynWriteable;
use polars::prelude::sync_on_close::SyncOnCloseType;
use polars_utils::file::ClosableFile;
use polars_utils::mmap::MemSlice;

use crate::RbResult;
use crate::error::RbPolarsErr;
use crate::prelude::resolve_homedir;

#[derive(Clone)]
pub struct RbFileLikeObject {
    inner: Opaque<Value>,
}

impl DynWriteable for RbFileLikeObject {
    fn as_dyn_write(&self) -> &(dyn io::Write + Send + 'static) {
        self as _
    }
    fn as_mut_dyn_write(&mut self) -> &mut (dyn io::Write + Send + 'static) {
        self as _
    }
    fn close(self: Box<Self>) -> io::Result<()> {
        Ok(())
    }
    fn sync_on_close(&mut self, _sync_on_close: SyncOnCloseType) -> io::Result<()> {
        Ok(())
    }
}

/// Wraps a `Value`, and implements read, seek, and write for it.
impl RbFileLikeObject {
    /// Creates an instance of a `RbFileLikeObject` from a `Value`.
    /// To assert the object has the required methods methods,
    /// instantiate it with `RbFileLikeObject::require`
    pub fn new(object: Value) -> Self {
        RbFileLikeObject {
            inner: object.into(),
        }
    }

    pub fn as_bytes(&self) -> bytes::Bytes {
        self.as_file_buffer().into_inner().into()
    }

    pub fn as_file_buffer(&self) -> Cursor<Vec<u8>> {
        let bytes = Ruby::get()
            .unwrap()
            .get_inner(self.inner)
            .funcall::<_, _, RString>("read", ())
            .expect("no read method found");

        let buf = unsafe { bytes.as_slice() }.to_vec();

        Cursor::new(buf)
    }

    /// Same as `RbFileLikeObject::new`, but validates that the underlying
    /// ruby object has a `read`, `write`, and `seek` methods in respect to parameters.
    /// Will return a `TypeError` if object does not have `read`, `seek`, and `write` methods.
    pub fn with_requirements(object: Value, read: bool, write: bool, seek: bool) -> RbResult<Self> {
        if read && !object.respond_to("read", false)? {
            return Err(Error::new(
                exception::type_error(),
                "Object does not have a .read() method.",
            ));
        }

        if seek && !object.respond_to("seek", false)? {
            return Err(Error::new(
                exception::type_error(),
                "Object does not have a .seek() method.",
            ));
        }

        if write && !object.respond_to("write", false)? {
            return Err(Error::new(
                exception::type_error(),
                "Object does not have a .write() method.",
            ));
        }

        Ok(RbFileLikeObject::new(object))
    }
}

/// Extracts a string repr from, and returns an IO error to send back to rust.
fn rberr_to_io_err(e: Error) -> io::Error {
    io::Error::other(e.to_string())
}

impl Read for RbFileLikeObject {
    fn read(&mut self, mut buf: &mut [u8]) -> Result<usize, io::Error> {
        let bytes = Ruby::get()
            .unwrap()
            .get_inner(self.inner)
            .funcall::<_, _, RString>("read", (buf.len(),))
            .map_err(rberr_to_io_err)?;

        buf.write_all(unsafe { bytes.as_slice() })?;

        Ok(bytes.len())
    }
}

impl Write for RbFileLikeObject {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        let rbbytes = RString::from_slice(buf);

        let number_bytes_written = Ruby::get()
            .unwrap()
            .get_inner(self.inner)
            .funcall::<_, _, usize>("write", (rbbytes,))
            .map_err(rberr_to_io_err)?;

        Ok(number_bytes_written)
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        Ruby::get()
            .unwrap()
            .get_inner(self.inner)
            .funcall::<_, _, Value>("flush", ())
            .map_err(rberr_to_io_err)?;

        Ok(())
    }
}

impl Seek for RbFileLikeObject {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, io::Error> {
        let (whence, offset) = match pos {
            SeekFrom::Start(i) => (0, i as i64),
            SeekFrom::Current(i) => (1, i),
            SeekFrom::End(i) => (2, i),
        };

        let new_position = Ruby::get()
            .unwrap()
            .get_inner(self.inner)
            .funcall("seek", (offset, whence))
            .map_err(rberr_to_io_err)?;

        Ok(new_position)
    }
}

pub trait FileLike: Read + Write + Seek {}

impl FileLike for File {}
impl FileLike for ClosableFile {}
impl FileLike for RbFileLikeObject {}

pub enum EitherRustRubyFile {
    Rb(RbFileLikeObject),
    Rust(ClosableFile),
}

impl EitherRustRubyFile {
    pub fn into_dyn(self) -> Box<dyn FileLike> {
        match self {
            EitherRustRubyFile::Rb(f) => Box::new(f),
            EitherRustRubyFile::Rust(f) => Box::new(f),
        }
    }

    pub(crate) fn into_writeable(self) -> Box<dyn DynWriteable> {
        match self {
            Self::Rb(f) => Box::new(f),
            Self::Rust(f) => Box::new(f),
        }
    }

    pub fn into_dyn_writeable(self) -> Box<dyn Write> {
        match self {
            EitherRustRubyFile::Rb(f) => Box::new(f),
            EitherRustRubyFile::Rust(f) => Box::new(f),
        }
    }
}

pub enum RubyScanSourceInput {
    Buffer(MemSlice),
    Path(PathBuf),
    #[allow(dead_code)]
    File(File),
}

pub(crate) fn try_get_rbfile(
    rb_f: Value,
    write: bool,
) -> RbResult<(EitherRustRubyFile, Option<PathBuf>)> {
    let f = RbFileLikeObject::with_requirements(rb_f, !write, write, !write)?;
    Ok((EitherRustRubyFile::Rb(f), None))
}

pub fn get_ruby_scan_source_input(rb_f: Value, write: bool) -> RbResult<RubyScanSourceInput> {
    if let Ok(file_path) = PathBuf::try_convert(rb_f) {
        // TODO resolve_homedir
        Ok(RubyScanSourceInput::Path(file_path))
    } else {
        let f = RbFileLikeObject::with_requirements(rb_f, !write, write, !write)?;
        Ok(RubyScanSourceInput::Buffer(MemSlice::from_bytes(
            f.as_bytes(),
        )))
    }
}

///
/// # Arguments
/// * `truncate` - open or create a new file.
pub fn get_either_file(rb_f: Value, truncate: bool) -> RbResult<EitherRustRubyFile> {
    if let Ok(rstring) = RString::try_convert(rb_f) {
        let s = unsafe { rstring.as_str() }?;
        let file_path = std::path::Path::new(&s);
        let file_path = resolve_homedir(&file_path);
        let f = if truncate {
            File::create(file_path).map_err(RbPolarsErr::from)?
        } else {
            polars_utils::open_file(&file_path).map_err(RbPolarsErr::from)?
        };
        Ok(EitherRustRubyFile::Rust(f.into()))
    } else {
        let f = RbFileLikeObject::with_requirements(rb_f, !truncate, truncate, !truncate)?;
        Ok(EitherRustRubyFile::Rb(f))
    }
}

pub fn get_file_like(f: Value, truncate: bool) -> RbResult<Box<dyn FileLike>> {
    Ok(get_either_file(f, truncate)?.into_dyn())
}

pub enum RbReadBytes {
    Bytes(RString),
    Other(Value),
}

pub fn read_if_bytesio(rb_f: Value) -> RbReadBytes {
    rb_f.funcall("read", ())
        .map(RbReadBytes::Bytes)
        .unwrap_or(RbReadBytes::Other(rb_f))
}

pub fn get_mmap_bytes_reader<'a>(rb_f: &'a RbReadBytes) -> RbResult<Box<dyn MmapBytesReader + 'a>> {
    get_mmap_bytes_reader_and_path(rb_f).map(|t| t.0)
}

pub fn get_mmap_bytes_reader_and_path<'a>(
    rb_f: &'a RbReadBytes,
) -> RbResult<(Box<dyn MmapBytesReader + 'a>, Option<PathBuf>)> {
    match rb_f {
        RbReadBytes::Bytes(v) => Ok((Box::new(Cursor::new(unsafe { v.as_slice() })), None)),
        RbReadBytes::Other(v) => {
            let path = PathBuf::try_convert(*v)?;
            let f = File::open(&path)
                .map_err(|e| Error::new(exception::runtime_error(), e.to_string()))?;
            Ok((Box::new(f), Some(path)))
        }
    }
}

pub fn try_get_writeable(
    rb_f: Value,
    _cloud_options: Option<&CloudOptions>,
) -> RbResult<Box<dyn Write>> {
    Ok(get_either_file(rb_f, true)?.into_dyn_writeable())
}
