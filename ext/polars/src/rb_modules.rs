use magnus::{Module, RClass, RModule, Ruby, value::Lazy};

static POLARS: Lazy<RModule> = Lazy::new(|ruby| ruby.class_object().const_get("Polars").unwrap());
static UTILS: Lazy<RModule> = Lazy::new(|ruby| ruby.get_inner(&POLARS).const_get("Utils").unwrap());
static SERIES: Lazy<RClass> =
    Lazy::new(|ruby| ruby.get_inner(&POLARS).const_get("Series").unwrap());

pub(crate) fn polars() -> RModule {
    Ruby::get().unwrap().get_inner(&POLARS)
}

pub(crate) fn pl_utils(rb: &Ruby) -> RModule {
    rb.get_inner(&UTILS)
}

pub(crate) fn pl_series(rb: &Ruby) -> RClass {
    rb.get_inner(&SERIES)
}

static BIGDECIMAL: Lazy<RClass> =
    Lazy::new(|ruby| ruby.class_object().const_get("BigDecimal").unwrap());
static DATE: Lazy<RClass> = Lazy::new(|ruby| ruby.class_object().const_get("Date").unwrap());
static DATETIME: Lazy<RClass> =
    Lazy::new(|ruby| ruby.class_object().const_get("DateTime").unwrap());

pub(crate) fn bigdecimal() -> RClass {
    Ruby::get().unwrap().get_inner(&BIGDECIMAL)
}

pub(crate) fn date() -> RClass {
    Ruby::get().unwrap().get_inner(&DATE)
}

pub(crate) fn datetime() -> RClass {
    Ruby::get().unwrap().get_inner(&DATETIME)
}
