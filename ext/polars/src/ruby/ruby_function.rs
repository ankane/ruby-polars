use magnus::{Value, Ruby};

pub struct RubyObject(pub Value);

pub type RubyFunction = RubyObject;

impl From<Value> for RubyObject {
    fn from(value: Value) -> Self {
        Ruby::get_with(value).gc_register_address(&value);
        Self(value)
    }
}
