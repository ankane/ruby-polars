use magnus::{Value, value::Opaque};

pub struct RubyObject(pub Opaque<Value>);

pub type RubyFunction = RubyObject;

impl From<Value> for RubyObject {
    fn from(value: Value) -> Self {
        Self(value.into())
    }
}
