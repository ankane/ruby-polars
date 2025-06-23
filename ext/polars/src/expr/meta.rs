use magnus::RArray;
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
        Ok(RArray::from_iter(
            exprs.iter().map(|e| RbExpr::from(e.clone())),
        ))
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

    pub fn _meta_selector_add(&self, other: &RbExpr) -> RbResult<RbExpr> {
        let out = self
            .inner
            .clone()
            .meta()
            ._selector_add(other.inner.clone())
            .map_err(RbPolarsErr::from)?;
        Ok(out.into())
    }

    pub fn _meta_selector_sub(&self, other: &RbExpr) -> RbResult<RbExpr> {
        let out = self
            .inner
            .clone()
            .meta()
            ._selector_sub(other.inner.clone())
            .map_err(RbPolarsErr::from)?;
        Ok(out.into())
    }

    pub fn _meta_selector_and(&self, other: &RbExpr) -> RbResult<RbExpr> {
        let out = self
            .inner
            .clone()
            .meta()
            ._selector_and(other.inner.clone())
            .map_err(RbPolarsErr::from)?;
        Ok(out.into())
    }

    pub fn _meta_as_selector(&self) -> RbExpr {
        self.inner.clone().meta()._into_selector().into()
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
