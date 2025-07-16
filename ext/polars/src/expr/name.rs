use magnus::{Ruby, block::Proc, value::Opaque};
use polars::prelude::*;
use polars_utils::format_pl_smallstr;

use crate::RbExpr;

impl RbExpr {
    pub fn name_keep(&self) -> Self {
        self.inner.clone().name().keep().into()
    }

    pub fn name_map(&self, lambda: Proc) -> Self {
        let lambda = Opaque::from(lambda);
        self.inner
            .clone()
            .name()
            .map(move |name| {
                let lambda = Ruby::get().unwrap().get_inner(lambda);
                let out = lambda.call::<_, String>((name.as_str(),));
                match out {
                    Ok(out) => Ok(format_pl_smallstr!("{}", out)),
                    Err(e) => Err(PolarsError::ComputeError(
                        format!("Ruby function in 'name.map' produced an error: {e}.").into(),
                    )),
                }
            })
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
}
