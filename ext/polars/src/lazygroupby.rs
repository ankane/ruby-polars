use std::sync::Arc;

use magnus::{RArray, Value};
use polars::lazy::frame::{LazyFrame, LazyGroupBy};
use polars::prelude::{PlanCallback, Schema};

use crate::conversion::Wrap;
use crate::error::RbPolarsErr;
use crate::expr::ToExprs;
use crate::ruby::plan_callback::PlanCallbackExt;
use crate::ruby::ruby_function::RubyObject;
use crate::{RbLazyFrame, RbResult};

#[magnus::wrap(class = "Polars::RbLazyGroupBy")]
pub struct RbLazyGroupBy {
    pub lgb: Option<LazyGroupBy>,
}

impl RbLazyGroupBy {
    pub fn having(&self, predicates: RArray) -> RbResult<RbLazyGroupBy> {
        let mut lgb = self.lgb.clone().unwrap();
        let predicates = predicates.to_exprs()?;
        for predicate in predicates.into_iter() {
            lgb = lgb.having(predicate);
        }
        Ok(RbLazyGroupBy { lgb: Some(lgb) })
    }

    pub fn agg(&self, aggs: RArray) -> RbResult<RbLazyFrame> {
        let lgb = self.lgb.clone().unwrap();
        let aggs = aggs.to_exprs()?;
        Ok(lgb.agg(aggs).into())
    }

    pub fn head(&self, n: usize) -> RbLazyFrame {
        let lgb = self.lgb.clone().unwrap();
        lgb.head(Some(n)).into()
    }

    pub fn tail(&self, n: usize) -> RbLazyFrame {
        let lgb = self.lgb.clone().unwrap();
        lgb.tail(Some(n)).into()
    }

    pub fn map_groups(&self, lambda: Value, schema: Option<Wrap<Schema>>) -> RbResult<RbLazyFrame> {
        let lgb = self.lgb.clone().unwrap();
        let schema = match schema {
            Some(schema) => Arc::new(schema.0),
            None => LazyFrame::from(lgb.logical_plan.clone())
                .collect_schema()
                .map_err(RbPolarsErr::from)?,
        };

        let function = RubyObject(lambda);

        Ok(lgb.apply(PlanCallback::new_ruby(function), schema).into())
    }
}
