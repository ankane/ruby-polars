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
        F: Send + FnOnce() -> T,
        T: Send;
}

impl GvlExt for Ruby {
    fn attach<T, F>(func: F) -> T
    where
        F: FnOnce(&Ruby) -> T,
    {
        match Ruby::get() {
            Ok(rb) => func(&rb),
            Err(RubyUnavailableError::GvlUnlocked) => {
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
            }
            Err(RubyUnavailableError::NonRubyThread) => panic!("Non-Ruby thread"),
        }
    }

    fn detach<T, F>(&self, func: F) -> T
    where
        F: Send + FnOnce() -> T,
        T: Send,
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
