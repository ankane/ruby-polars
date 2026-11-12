use magnus::{Value, Ruby, value::Opaque};

pub struct RubyObject(pub Opaque<Value>);

pub type RubyFunction = RubyObject;

impl From<Value> for RubyObject {
    fn from(value: Value) -> Self {
        Ruby::get_with(value).gc_register_address(&value);
        Self(value.into())
    }
}
