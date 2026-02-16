use magnus::{Module, RClass, Ruby, value::Lazy};

static BIGDECIMAL: Lazy<RClass> =
    Lazy::new(|rb| rb.class_object().const_get("BigDecimal").unwrap());
static DATE: Lazy<RClass> = Lazy::new(|rb| rb.class_object().const_get("Date").unwrap());
static DATETIME: Lazy<RClass> = Lazy::new(|rb| rb.class_object().const_get("DateTime").unwrap());

pub(crate) fn bigdecimal(rb: &Ruby) -> RClass {
    rb.get_inner(&BIGDECIMAL)
}

pub(crate) fn date(rb: &Ruby) -> RClass {
    rb.get_inner(&DATE)
}

pub(crate) fn datetime(rb: &Ruby) -> RClass {
    rb.get_inner(&DATETIME)
}
