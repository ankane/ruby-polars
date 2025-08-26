use magnus::{IntoValue, Module, RClass, RModule, Ruby, Value, prelude::*};

use crate::RbResult;

pub trait Element: IntoValue {
    fn class_name() -> &'static str;
}

macro_rules! create_element {
    ($type:ty, $name:expr) => {
        impl Element for $type {
            fn class_name() -> &'static str {
                $name
            }
        }
    };
}

create_element!(i8, "Int8");
create_element!(i16, "Int16");
create_element!(i32, "Int32");
create_element!(i64, "Int64");
create_element!(u8, "UInt8");
create_element!(u16, "UInt16");
create_element!(u32, "UInt32");
create_element!(u64, "UInt64");
create_element!(f32, "SFloat");
create_element!(f64, "DFloat");
create_element!(bool, "Bit");

impl<T> Element for Option<T>
where
    Option<T>: IntoValue,
{
    fn class_name() -> &'static str {
        "RObject"
    }
}

pub struct RbArray1<T>(T);

impl<T: Element> RbArray1<T> {
    pub fn from_iter<I>(values: I) -> RbResult<Value>
    where
        I: IntoIterator<Item = T>,
    {
        Ruby::get()
            .unwrap()
            .class_object()
            .const_get::<_, RModule>("Numo")?
            .const_get::<_, RClass>(T::class_name())?
            .funcall("cast", (Ruby::get().unwrap().ary_from_iter(values),))
    }
}
