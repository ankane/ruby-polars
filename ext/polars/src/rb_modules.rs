use magnus::{class, memoize, Module, RClass, RModule};

pub(crate) fn polars() -> RModule {
    *memoize!(RModule: class::object().const_get("Polars").unwrap())
}

pub(crate) fn series() -> RClass {
    *memoize!(RClass: polars().const_get("Series").unwrap())
}

pub(crate) fn utils() -> RModule {
    *memoize!(RModule: polars().const_get("Utils").unwrap())
}

pub(crate) fn bigdecimal() -> RClass {
    *memoize!(RClass: class::object().const_get("BigDecimal").unwrap())
}

pub(crate) fn date() -> RClass {
    *memoize!(RClass: class::object().const_get("Date").unwrap())
}

pub(crate) fn datetime() -> RClass {
    *memoize!(RClass: class::object().const_get("DateTime").unwrap())
}
