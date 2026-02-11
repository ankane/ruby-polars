use std::any::Any;
use std::fs::File;
use std::io;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::mpsc::{SyncSender, sync_channel};

use magnus::{Error, RString, Ruby, Value, error::RubyUnavailableError, prelude::*, value::Opaque};
use polars::io::mmap::MmapBytesReader;
use polars::prelude::PlRefPath;
use polars::prelude::file::{Writeable, WriteableTrait};
use polars_buffer::Buffer;
use polars_utils::create_file;

use crate::error::RbPolarsErr;
use crate::prelude::resolve_homedir;
use crate::utils::{RubyAttach, to_rb_err};
use crate::{RbErr, RbResult};

pub struct RbFileLikeObject {
    inner: Opaque<Value>,
    expects_str: bool,
    has_flush: bool,
}

impl WriteableTrait for RbFileLikeObject {
    fn close(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn sync_all(&self) -> std::io::Result<()> {
        self.flush()
    }

    fn sync_data(&self) -> std::io::Result<()> {
        self.flush()
    }
}

impl Clone for RbFileLikeObject {
    fn clone(&self) -> Self {
        // Skip attach
        Self {
            inner: self.inner,
            expects_str: self.expects_str,
            has_flush: self.has_flush,
        }
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

    pub(crate) fn to_buffer(&self) -> Buffer<u8> {
        Ruby::attach(|rb| {
            let bytes = rb
                .get_inner(self.inner)
                .funcall::<_, _, RString>("read", ())
                .expect("no read method found");

            let b = unsafe { bytes.as_slice() }.to_vec();
            Buffer::from_vec(b)
        })
    }

    /// Same as `RbFileLikeObject::new`, but validates that the underlying
    /// ruby object has a `read`, `write`, and `seek` methods in respect to parameters.
    /// Will return a `TypeError` if object does not have `read`, `seek`, and `write` methods.
    pub fn ensure_requirements(object: Value, read: bool, write: bool, seek: bool) -> RbResult<()> {
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

        Ok(())
    }

    fn flush(&self) -> std::io::Result<()> {
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
        if is_non_ruby_thread() {
            let (sender, receiver) = sync_channel(0);
            POLARS_RUBY_SENDER
                .get()
                .unwrap()
                .send((self.clone(), buf.to_vec(), sender))
                .unwrap();
            return *receiver.recv().unwrap().downcast().unwrap();
        }

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
        if is_non_ruby_thread() {
            // handled in write for now
            return Ok(());
        }

        Self::flush(self)
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
impl FileLike for RbFileLikeObject {}

pub enum EitherRustRubyFile {
    Rb(RbFileLikeObject),
    Rust(std::fs::File),
}

impl EitherRustRubyFile {
    pub fn into_dyn(self) -> Box<dyn FileLike> {
        match self {
            EitherRustRubyFile::Rb(f) => Box::new(f),
            EitherRustRubyFile::Rust(f) => Box::new(f),
        }
    }

    fn into_scan_source_input(self) -> RubyScanSourceInput {
        match self {
            EitherRustRubyFile::Rb(f) => RubyScanSourceInput::Buffer(f.to_buffer()),
            EitherRustRubyFile::Rust(f) => RubyScanSourceInput::File(f),
        }
    }

    pub(crate) fn into_writeable(self) -> Writeable {
        match self {
            Self::Rb(f) => Writeable::Dyn(Box::new(f)),
            Self::Rust(f) => Writeable::Local(f),
        }
    }
}

pub enum RubyScanSourceInput {
    Buffer(Buffer<u8>),
    Path(PlRefPath),
    File(std::fs::File),
}

pub(crate) fn try_get_rbfile(
    rb: &Ruby,
    rb_f: Value,
    write: bool,
) -> RbResult<(EitherRustRubyFile, Option<PathBuf>)> {
    RbFileLikeObject::ensure_requirements(rb_f, !write, write, !write)?;
    let expects_str = false;
    let has_flush = rb_f.respond_to("flush", false)?;
    let f = RbFileLikeObject::new(rb_f, expects_str, has_flush);

    // TODO move
    if write {
        start_background_thread(rb);
    }

    Ok((EitherRustRubyFile::Rb(f), None))
}

pub fn get_ruby_scan_source_input(rb_f: Value, write: bool) -> RbResult<RubyScanSourceInput> {
    Ruby::attach(|rb| {
        if let Ok(s) = PathBuf::try_convert(rb_f) {
            let file_path =
                PlRefPath::try_from_path(resolve_homedir(&s).as_ref()).map_err(to_rb_err)?;

            Ok(RubyScanSourceInput::Path(file_path))
        } else {
            Ok(try_get_rbfile(rb, rb_f, write)?.0.into_scan_source_input())
        }
    })
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
            Ok((EitherRustRubyFile::Rust(f), Some(file_path.into_owned())))
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
            (EitherRustRubyFile::Rb(f), path) => Ok((Box::new(Cursor::new(f.to_buffer())), path)),
        },
    }
}

#[allow(clippy::type_complexity)]
static POLARS_RUBY_SENDER: OnceLock<
    SyncSender<(RbFileLikeObject, Vec<u8>, SyncSender<Box<dyn Any + Send>>)>,
> = OnceLock::new();

// TODO figure out better approach
fn start_background_thread(rb: &Ruby) {
    POLARS_RUBY_SENDER.get_or_init(|| {
        let (sender, receiver) =
            sync_channel::<(RbFileLikeObject, Vec<u8>, SyncSender<Box<dyn Any + Send>>)>(0);

        // TODO save reference to thread?
        rb.thread_create_from_fn(move |rb2| {
            loop {
                match receiver.try_recv() {
                    Ok((mut f, buf, sender2)) => {
                        let result = f.write(&buf);
                        if result.is_ok() {
                            // flush writes for now
                            f.flush().unwrap();
                        }
                        sender2.send(Box::new(result)).unwrap();
                    }
                    Err(_) => {
                        rb2.thread_sleep(std::time::Duration::from_millis(1))?;
                    }
                }
            }

            #[allow(unreachable_code)]
            Ok(())
        });

        sender
    });
}

fn is_non_ruby_thread() -> bool {
    matches!(Ruby::get(), Err(RubyUnavailableError::NonRubyThread))
}
