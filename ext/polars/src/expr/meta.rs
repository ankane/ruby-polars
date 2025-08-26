use magnus::{RArray, Ruby};
use polars::prelude::Schema;

use crate::{RbExpr, RbPolarsErr, RbResult, Wrap};

impl RbExpr {
    pub fn meta_eq(&self, other: &RbExpr) -> bool {
        self.inner == other.inner
    }

    pub fn meta_pop(&self, schema: Option<Wrap<Schema>>) -> RbResult<RArray> {
        let schema = schema.as_ref().map(|s| &s.0);
        let exprs = self
            .inner
            .clone()
            .meta()
            .pop(schema)
            .map_err(RbPolarsErr::from)?;
        Ok(Ruby::get()
            .unwrap()
            .ary_from_iter(exprs.iter().map(|e| RbExpr::from(e.clone()))))
    }

    pub fn meta_root_names(&self) -> Vec<String> {
        self.inner
            .clone()
            .meta()
            .root_names()
            .iter()
            .map(|name| name.to_string())
            .collect()
    }

    pub fn meta_output_name(&self) -> RbResult<String> {
        let name = self
            .inner
            .clone()
            .meta()
            .output_name()
            .map_err(RbPolarsErr::from)?;
        Ok(name.to_string())
    }

    pub fn meta_undo_aliases(&self) -> RbExpr {
        self.inner.clone().meta().undo_aliases().into()
    }

    pub fn meta_has_multiple_outputs(&self) -> bool {
        self.inner.clone().meta().has_multiple_outputs()
    }

    pub fn meta_is_column(&self) -> bool {
        self.inner.clone().meta().is_column()
    }

    pub fn meta_is_regex_projection(&self) -> bool {
        self.inner.clone().meta().is_regex_projection()
    }

    pub fn meta_is_column_selection(&self, allow_aliasing: bool) -> bool {
        self.inner
            .clone()
            .meta()
            .is_column_selection(allow_aliasing)
    }

    pub fn meta_is_literal(&self, allow_aliasing: bool) -> bool {
        self.inner.clone().meta().is_literal(allow_aliasing)
    }

    fn compute_tree_format(
        &self,
        display_as_dot: bool,
        schema: Option<Wrap<Schema>>,
    ) -> RbResult<String> {
        let e = self
            .inner
            .clone()
            .meta()
            .into_tree_formatter(display_as_dot, schema.as_ref().map(|s| &s.0))
            .map_err(RbPolarsErr::from)?;
        Ok(format!("{e}"))
    }

    pub fn meta_tree_format(&self, schema: Option<Wrap<Schema>>) -> RbResult<String> {
        self.compute_tree_format(false, schema)
    }
}
