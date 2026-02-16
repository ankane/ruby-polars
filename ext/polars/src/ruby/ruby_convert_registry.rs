use std::any::Any;
use std::ops::Deref;
use std::sync::{Arc, LazyLock, RwLock};

use magnus::{Value, value::Opaque};

use crate::RbResult;

pub type FromRuby = Arc<dyn Fn(Opaque<Value>) -> RbResult<Box<dyn Any>> + Send + Sync>;
pub type ToRuby = Arc<dyn for<'a> Fn(&'a dyn Any) -> RbResult<Value> + Send + Sync>;

#[derive(Clone)]
pub struct FromRubyConvertRegistry {
    #[allow(unused)]
    pub file_provider_result: FromRuby,
    pub series: FromRuby,
    pub df: FromRuby,
    pub dsl_plan: FromRuby,
    pub schema: FromRuby,
}

#[derive(Clone)]
pub struct ToRubyConvertRegistry {
    pub df: ToRuby,
    pub series: ToRuby,
    pub dsl_plan: ToRuby,
    pub schema: ToRuby,
}

#[derive(Clone)]
pub struct RubyConvertRegistry {
    pub from_rb: FromRubyConvertRegistry,
    pub to_rb: ToRubyConvertRegistry,
}

static RUBY_CONVERT_REGISTRY: LazyLock<RwLock<Option<RubyConvertRegistry>>> =
    LazyLock::new(Default::default);

pub fn get_ruby_convert_registry() -> RubyConvertRegistry {
    RUBY_CONVERT_REGISTRY
        .deref()
        .read()
        .unwrap()
        .as_ref()
        .unwrap()
        .clone()
}

pub fn register_converters(registry: RubyConvertRegistry) {
    *RUBY_CONVERT_REGISTRY.deref().write().unwrap() = Some(registry);
}
