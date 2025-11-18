use std::fs::File;
use std::io;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use magnus::{Error, RString, Ruby, Value, prelude::*, value::Opaque};
use polars::io::mmap::MmapBytesReader;
use polars::prelude::PlPath;
use polars::prelude::file::DynWriteable;
use polars::prelude::sync_on_close::SyncOnCloseType;
use polars_utils::create_file;
use polars_utils::file::{ClosableFile, WriteClose};
use polars_utils::mmap::MemSlice;

use crate::error::RbPolarsErr;
use crate::prelude::resolve_homedir;
use crate::utils::RubyAttach;
use crate::{RbErr, RbResult};

pub struct RbFileLikeObject {
    inner: Opaque<Value>,
    expects_str: bool,
    has_flush: bool,
}

impl WriteClose for RbFileLikeObject {}

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

impl Clone for RbFileLikeObject {
    fn clone(&self) -> Self {
        Ruby::attach(|_rb| Self {
            // TODO clone
            inner: self.inner,
            expects_str: self.expects_str,
            has_flush: self.has_flush,
        })
    }
}

/// Wraps a `Value`, and implements read, seek, and write for it.
impl RbFileLikeObject {
    /// Creates an instance of a `RbFileLikeObject` from a `Value`.
    /// To assert the object has the required methods methods,
    /// instantiate it with `RbFileLikeObject::require`
    pub fn new(object: Value, expects_str: bool, has_flush: bool) -> Self {
        RbFileLikeObject {
            inner: object.into(),
            expects_str,
            has_flush,
        }
    }

    pub(crate) fn to_memslice(&self) -> MemSlice {
        Ruby::attach(|rb| {
            let bytes = rb
                .get_inner(self.inner)
                .funcall::<_, _, RString>("read", ())
                .expect("no read method found");

            let b = unsafe { bytes.as_slice() }.to_vec();
            MemSlice::from_vec(b)
        })
    }

    /// Same as `RbFileLikeObject::new`, but validates that the underlying
    /// ruby object has a `read`, `write`, and `seek` methods in respect to parameters.
    /// Will return a `TypeError` if object does not have `read`, `seek`, and `write` methods.
    pub fn ensure_requirements(
        object: Value,
        read: bool,
        write: bool,
        seek: bool,
    ) -> RbResult<Self> {
        let ruby = Ruby::get_with(object);

        if read && !object.respond_to("read", false)? {
            return Err(Error::new(
                ruby.exception_type_error(),
                "Object does not have a .read() method.",
            ));
        }

        if seek && !object.respond_to("seek", false)? {
            return Err(Error::new(
                ruby.exception_type_error(),
                "Object does not have a .seek() method.",
            ));
        }

        if write && !object.respond_to("write", false)? {
            return Err(Error::new(
                ruby.exception_type_error(),
                "Object does not have a .write() method.",
            ));
        }

        // TODO fix
        let expects_str = false;
        let has_flush = object.respond_to("write", false)?;
        Ok(RbFileLikeObject::new(object, expects_str, has_flush))
    }
}

/// Extracts a string repr from, and returns an IO error to send back to rust.
fn rberr_to_io_err(e: RbErr) -> io::Error {
    Ruby::attach(|_rb| io::Error::other(e.to_string()))
}

impl Read for RbFileLikeObject {
    fn read(&mut self, mut buf: &mut [u8]) -> Result<usize, io::Error> {
        Ruby::attach(|rb| {
            let bytes = rb
                .get_inner(self.inner)
                .funcall::<_, _, RString>("read", (buf.len(),))
                .map_err(rberr_to_io_err)?;

            buf.write_all(unsafe { bytes.as_slice() })?;

            Ok(bytes.len())
        })
    }
}

impl Write for RbFileLikeObject {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        let expects_str = self.expects_str;

        Ruby::attach(|rb| {
            if expects_str {
                todo!();
            }

            let rbbytes = rb.str_from_slice(buf);

            let number_bytes_written = rb
                .get_inner(self.inner)
                .funcall::<_, _, usize>("write", (rbbytes,))
                .map_err(rberr_to_io_err)?;

            Ok(number_bytes_written)
        })
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        if self.has_flush {
            Ruby::attach(|rb| {
                rb.get_inner(self.inner)
                    .funcall::<_, _, Value>("flush", ())
                    .map_err(rberr_to_io_err)
            })?;
        }

        Ok(())
    }
}

impl Seek for RbFileLikeObject {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, io::Error> {
        Ruby::attach(|rb| {
            let (whence, offset) = match pos {
                SeekFrom::Start(i) => (0, i as i64),
                SeekFrom::Current(i) => (1, i),
                SeekFrom::End(i) => (2, i),
            };

            let inner = rb.get_inner(self.inner);

            inner
                .funcall::<_, _, Value>("seek", (offset, whence))
                .map_err(rberr_to_io_err)?;

            inner.funcall("tell", ()).map_err(rberr_to_io_err)
        })
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

    #[allow(dead_code)]
    fn into_scan_source_input(self) -> RubyScanSourceInput {
        match self {
            EitherRustRubyFile::Rb(f) => RubyScanSourceInput::Buffer(f.to_memslice()),
            EitherRustRubyFile::Rust(f) => RubyScanSourceInput::File(f),
        }
    }

    pub(crate) fn into_writeable(self) -> Box<dyn DynWriteable> {
        match self {
            Self::Rb(f) => Box::new(f),
            Self::Rust(f) => Box::new(f),
        }
    }
}

pub enum RubyScanSourceInput {
    Buffer(MemSlice),
    Path(PlPath),
    #[allow(dead_code)]
    File(ClosableFile),
}

pub(crate) fn try_get_rbfile(
    _rb: &Ruby,
    rb_f: Value,
    write: bool,
) -> RbResult<(EitherRustRubyFile, Option<PathBuf>)> {
    let f = RbFileLikeObject::ensure_requirements(rb_f, !write, write, !write)?;
    Ok((EitherRustRubyFile::Rb(f), None))
}

pub fn get_ruby_scan_source_input(rb_f: Value, write: bool) -> RbResult<RubyScanSourceInput> {
    // Ruby::attach(|_rb| {
        println!("get_ruby_scan_source_input");
        if let Ok(s) = String::try_convert(rb_f) {
            let mut file_path = PlPath::new(&s);
            if let Some(p) = file_path.as_ref().as_local_path()
                && p.starts_with("~/")
            {
                file_path = PlPath::Local(resolve_homedir(&p).into());
            }
            println!("file_path: {:?}", file_path);
            Ok(RubyScanSourceInput::Path(file_path))
        } else {
            println!("other path");
            let f = RbFileLikeObject::ensure_requirements(rb_f, !write, write, !write)?;
            Ok(RubyScanSourceInput::Buffer(f.to_memslice()))
        }
    // })
}

pub fn get_either_buffer_or_path(
    rb_f: Value,
    write: bool,
) -> RbResult<(EitherRustRubyFile, Option<PathBuf>)> {
    Ruby::attach(|rb| {
        if let Ok(rstring) = RString::try_convert(rb_f) {
            let s = unsafe { rstring.as_str() }?;
            let file_path = std::path::Path::new(&s);
            let file_path = resolve_homedir(&file_path);
            let f = if write {
                create_file(&file_path).map_err(RbPolarsErr::from)?
            } else {
                polars_utils::open_file(&file_path).map_err(RbPolarsErr::from)?
            };
            Ok((EitherRustRubyFile::Rust(f.into()), Some(file_path)))
        } else {
            try_get_rbfile(rb, rb_f, write)
        }
    })
}

///
/// # Arguments
/// * `write` - open for writing; will truncate existing file and create new file if not.
pub fn get_either_file(rb_f: Value, write: bool) -> RbResult<EitherRustRubyFile> {
    Ok(get_either_buffer_or_path(rb_f, write)?.0)
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
        RbReadBytes::Other(v) => match get_either_buffer_or_path(*v, false)? {
            (EitherRustRubyFile::Rust(f), path) => Ok((Box::new(f), path)),
            (EitherRustRubyFile::Rb(f), path) => Ok((Box::new(Cursor::new(f.to_memslice())), path)),
        },
    }
}
