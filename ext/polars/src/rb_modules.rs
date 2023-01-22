use magnus::{class, memoize, Module, RClass, RModule};

pub(crate) fn polars() -> RModule {
    *memoize!(RModule: class::object().const_get("Polars").unwrap())
}

pub(crate) fn series() -> RClass {
    *memoize!(RClass: polars().const_get("Series").unwrap())
}

pub(crate) fn date() -> RClass {
    *memoize!(RClass: class::object().const_get("Date").unwrap())
}
