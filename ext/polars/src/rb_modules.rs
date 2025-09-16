use magnus::{ExceptionClass, Module, RClass, RModule, Ruby, value::Lazy};

static POLARS: Lazy<RModule> = Lazy::new(|ruby| ruby.class_object().const_get("Polars").unwrap());

pub(crate) fn polars() -> RModule {
    Ruby::get().unwrap().get_inner(&POLARS)
}

static SERIES: Lazy<RClass> =
    Lazy::new(|ruby| ruby.get_inner(&POLARS).const_get("Series").unwrap());

pub(crate) fn pl_series() -> RClass {
    Ruby::get().unwrap().get_inner(&SERIES)
}

static UTILS: Lazy<RModule> = Lazy::new(|ruby| ruby.get_inner(&POLARS).const_get("Utils").unwrap());

pub(crate) fn pl_utils() -> RModule {
    Ruby::get().unwrap().get_inner(&UTILS)
}

static BIGDECIMAL: Lazy<RClass> =
    Lazy::new(|ruby| ruby.class_object().const_get("BigDecimal").unwrap());

pub(crate) fn bigdecimal() -> RClass {
    Ruby::get().unwrap().get_inner(&BIGDECIMAL)
}

static DATE: Lazy<RClass> = Lazy::new(|ruby| ruby.class_object().const_get("Date").unwrap());

pub(crate) fn date() -> RClass {
    Ruby::get().unwrap().get_inner(&DATE)
}

static DATETIME: Lazy<RClass> =
    Lazy::new(|ruby| ruby.class_object().const_get("DateTime").unwrap());

pub(crate) fn datetime() -> RClass {
    Ruby::get().unwrap().get_inner(&DATETIME)
}

static ERROR: Lazy<ExceptionClass> =
    Lazy::new(|ruby| ruby.get_inner(&POLARS).const_get("Error").unwrap());

pub(crate) fn error() -> ExceptionClass {
    Ruby::get().unwrap().get_inner(&ERROR)
}

static COMPUTE_ERROR: Lazy<ExceptionClass> =
    Lazy::new(|ruby| ruby.get_inner(&POLARS).const_get("ComputeError").unwrap());

pub(crate) fn compute_error() -> ExceptionClass {
    Ruby::get().unwrap().get_inner(&COMPUTE_ERROR)
}

static INVALID_OPERATION_ERROR: Lazy<ExceptionClass> = Lazy::new(|ruby| {
    ruby.get_inner(&POLARS)
        .const_get("InvalidOperationError")
        .unwrap()
});

pub(crate) fn invalid_operation_error() -> ExceptionClass {
    Ruby::get().unwrap().get_inner(&INVALID_OPERATION_ERROR)
}
