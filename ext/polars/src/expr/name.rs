use magnus::Value;
use polars::prelude::*;

use crate::RbExpr;
use crate::ruby::plan_callback::PlanCallbackExt;
use crate::ruby::ruby_function::RubyObject;

impl RbExpr {
    pub fn name_keep(&self) -> Self {
        self.inner.clone().name().keep().into()
    }

    pub fn name_map(&self, lambda: Value) -> Self {
        self.inner
            .clone()
            .name()
            .map(PlanCallback::new_ruby(RubyObject::from(lambda)))
            .into()
    }

    pub fn name_prefix(&self, prefix: String) -> Self {
        self.inner.clone().name().prefix(&prefix).into()
    }

    pub fn name_suffix(&self, suffix: String) -> Self {
        self.inner.clone().name().suffix(&suffix).into()
    }

    pub fn name_to_lowercase(&self) -> Self {
        self.inner.clone().name().to_lowercase().into()
    }

    pub fn name_to_uppercase(&self) -> Self {
        self.inner.clone().name().to_uppercase().into()
    }

    pub fn name_replace(&self, pattern: String, value: String, literal: bool) -> Self {
        self.inner
            .clone()
            .name()
            .replace(&pattern, &value, literal)
            .into()
    }

    pub fn name_map_fields(&self, name_mapper: Value) -> Self {
        self.inner
            .clone()
            .name()
            .map_fields(PlanCallback::new_ruby(RubyObject::from(name_mapper)))
            .into()
    }

    pub fn name_prefix_fields(&self, prefix: String) -> Self {
        self.inner.clone().name().prefix_fields(&prefix).into()
    }

    pub fn name_suffix_fields(&self, suffix: String) -> Self {
        self.inner.clone().name().suffix_fields(&suffix).into()
    }
}
