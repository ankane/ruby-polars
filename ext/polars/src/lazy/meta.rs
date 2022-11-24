use crate::{RbExpr, RbPolarsErr, RbResult};

impl RbExpr {
    pub fn meta_pop(&self) -> Vec<RbExpr> {
        self.inner
            .clone()
            .meta()
            .pop()
            .into_iter()
            .map(|v| RbExpr::from(v))
            .collect()
    }

    pub fn meta_eq(&self, other: &RbExpr) -> bool {
        self.inner == other.inner
    }

    pub fn meta_roots(&self) -> Vec<String> {
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
}
