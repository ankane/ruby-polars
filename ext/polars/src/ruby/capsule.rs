use std::any::Any;
use std::ffi::CString;

#[magnus::wrap(class = "Polars::Capsule")]
pub struct RbCapsule {
    value: Box<dyn Any + Send>,
    name: Option<CString>,
}

impl RbCapsule {
    pub fn new<T: 'static + Send>(value: T, name: Option<CString>) -> Self {
        Self {
            value: Box::new(value),
            name,
        }
    }

    pub fn to_i(&self) -> usize {
        (&*self.value as *const dyn Any as *const ()) as usize
    }

    // TODO use &CStr when Magnus supports it
    // https://github.com/matsadler/magnus/pull/182
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref().map(|v| v.to_str().unwrap())
    }
}
