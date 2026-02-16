use magnus::Value;

#[derive(Debug)]
pub struct RubyObject(pub Value);

pub type RubyFunction = RubyObject;

impl From<Value> for RubyObject {
    fn from(value: Value) -> Self {
        Self(value)
    }
}
