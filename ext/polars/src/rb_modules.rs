use magnus::{define_module, memoize, Module, RClass, RModule};

pub(crate) fn polars() -> RModule {
    *memoize!(RModule: define_module("Polars").unwrap())
}

pub(crate) fn series() -> RClass {
    *memoize!(RClass: polars().define_class("Series", Default::default()).unwrap())
}
