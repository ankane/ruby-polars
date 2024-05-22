use magnus::Value;
use polars_core::utils::try_get_supertype;

use crate::dataframe::RbDataFrame;

impl RbDataFrame {
    pub fn to_numo(&self) -> Option<Value> {
        let mut st = None;
        for s in self.df.borrow().iter() {
            let dt_i = s.dtype();
            match st {
                None => st = Some(dt_i.clone()),
                Some(ref mut st) => {
                    *st = try_get_supertype(st, dt_i).ok()?;
                }
            }
        }
        let _st = st?;

        // TODO
        None
    }
}
