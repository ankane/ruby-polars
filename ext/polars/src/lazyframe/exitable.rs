use magnus::Ruby;
use polars::prelude::*;

use super::RbLazyFrame;
use crate::utils::EnterPolarsExt;
use crate::{RbDataFrame, RbResult};

impl RbLazyFrame {
    pub fn collect_concurrently(ruby: &Ruby, self_: &Self) -> RbResult<RbInProcessQuery> {
        let ipq = ruby.enter_polars(|| {
            let ldf = self_.ldf.read().clone();
            ldf.collect_concurrently()
        })?;
        Ok(RbInProcessQuery { ipq })
    }
}

#[magnus::wrap(class = "Polars::RbInProcessQuery")]
#[repr(transparent)]
#[derive(Clone)]
pub struct RbInProcessQuery {
    pub ipq: InProcessQuery,
}

impl RbInProcessQuery {
    pub fn cancel(ruby: &Ruby, self_: &Self) -> RbResult<()> {
        ruby.enter_polars_ok(|| self_.ipq.cancel())
    }

    pub fn fetch(ruby: &Ruby, self_: &Self) -> RbResult<Option<RbDataFrame>> {
        let out = ruby.enter_polars(|| self_.ipq.fetch().transpose())?;
        Ok(out.map(|df| df.into()))
    }

    pub fn fetch_blocking(ruby: &Ruby, self_: &Self) -> RbResult<RbDataFrame> {
        let out = ruby.enter_polars(|| self_.ipq.fetch_blocking())?;
        Ok(out.into())
    }
}
