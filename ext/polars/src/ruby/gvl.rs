use std::ffi::c_void;
use std::ptr::null_mut;

use magnus::Ruby;
use magnus::error::RubyUnavailableError;
use rb_sys::{rb_thread_call_with_gvl, rb_thread_call_without_gvl};

pub trait GvlExt {
    fn attach<T, F>(func: F) -> T
    where
        F: FnOnce(&Ruby) -> T;

    fn detach<T, F>(&self, func: F) -> T
    where
        F: FnOnce() -> T + Send;
}

unsafe extern "C" {
    fn ruby_thread_has_gvl_p() -> std::ffi::c_int;
}

impl GvlExt for Ruby {
    fn attach<T, F>(func: F) -> T
    where
        F: FnOnce(&Ruby) -> T,
    {
        // recheck GVL state since cached value can be incorrect
        // https://github.com/matsadler/magnus/pull/161
        if let Ok(rb) = Ruby::get()
            && unsafe { ruby_thread_has_gvl_p() } != 0
        {
            func(&rb)
        } else if !matches!(Ruby::get(), Err(RubyUnavailableError::NonRubyThread)) {
            let mut data = CallbackData {
                func: Some(func),
                result: None,
            };

            unsafe {
                rb_thread_call_with_gvl(
                    Some(call_with_gvl::<F, T>),
                    &mut data as *mut _ as *mut c_void,
                );
            }

            data.result.unwrap()
        } else {
            panic!("Non-Ruby thread");
        }
    }

    fn detach<T, F>(&self, func: F) -> T
    where
        F: Send + FnOnce() -> T,
    {
        if std::env::var("POLARS_GVL").is_ok() {
            func()
        } else {
            let mut data = CallbackData {
                func: Some(func),
                result: None,
            };

            unsafe {
                rb_thread_call_without_gvl(
                    Some(call_without_gvl::<F, T>),
                    &mut data as *mut _ as *mut c_void,
                    None,
                    null_mut(),
                );
            }

            data.result.unwrap()
        }
    }
}

struct CallbackData<F, T> {
    func: Option<F>,
    result: Option<T>,
}

extern "C" fn call_without_gvl<F, T>(data: *mut c_void) -> *mut c_void
where
    F: FnOnce() -> T,
{
    let data = unsafe { &mut *(data as *mut CallbackData<F, T>) };
    let func = data.func.take().unwrap();
    data.result = Some(func());
    null_mut()
}

extern "C" fn call_with_gvl<F, T>(data: *mut c_void) -> *mut c_void
where
    F: FnOnce(&Ruby) -> T,
{
    let rb = Ruby::get().unwrap();
    let data = unsafe { &mut *(data as *mut CallbackData<F, T>) };
    let func = data.func.take().unwrap();
    data.result = Some(func(&rb));
    null_mut()
}
