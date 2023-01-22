use magnus::{define_module, memoize, Module, RClass, RModule};

pub(crate) fn module() -> RModule {
    *memoize!(RModule: define_module("Polars").unwrap())
}

pub(crate) fn series() -> RClass {
    *memoize!(RClass: module().define_class("Series", Default::default()).unwrap())
}
