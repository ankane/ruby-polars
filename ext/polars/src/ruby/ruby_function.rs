use magnus::Value;

#[derive(Debug)]
pub struct RubyObject(pub Value);

pub type RubyFunction = RubyObject;
