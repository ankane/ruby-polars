use magnus::{Module, RClass, Ruby, value::Lazy};

static DATE: Lazy<RClass> = Lazy::new(|rb| rb.class_object().const_get("Date").unwrap());
static DATETIME: Lazy<RClass> = Lazy::new(|rb| rb.class_object().const_get("DateTime").unwrap());

pub(crate) fn bigdecimal(rb: &Ruby) -> Option<RClass> {
    rb.class_object().const_get("BigDecimal").ok()
}

pub(crate) fn date(rb: &Ruby) -> RClass {
    rb.get_inner(&DATE)
}

pub(crate) fn datetime(rb: &Ruby) -> RClass {
    rb.get_inner(&DATETIME)
}
