use std::sync::{Arc, Mutex};

use magnus::{Ruby, TryConvert, Value};
use polars::prelude::sync_on_close::SyncOnCloseType;
use polars::prelude::{PlRefPath, SpecialEq};

use crate::prelude::Wrap;
use crate::ruby::gvl::RubyAttach;
use crate::{RbResult, RbValueError};

impl TryConvert for Wrap<polars_plan::dsl::SinkTarget> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        if let Ok(v) = String::try_convert(ob) {
            Ok(Wrap(polars::prelude::SinkTarget::Path(PlRefPath::new(&v))))
        } else {
            let writer = Ruby::attach(|rb| {
                let rb_f = ob;
                RbResult::Ok(
                    crate::file::try_get_rbfile(rb, rb_f, true)?
                        .0
                        .into_writeable(),
                )
            })?;

            Ok(Wrap(polars_plan::prelude::SinkTarget::Dyn(SpecialEq::new(
                Arc::new(Mutex::new(Some(writer))),
            ))))
        }
    }
}

impl TryConvert for Wrap<SyncOnCloseType> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "none" => SyncOnCloseType::None,
            "data" => SyncOnCloseType::Data,
            "all" => SyncOnCloseType::All,
            v => {
                return Err(RbValueError::new_err(format!(
                    "`sync_on_close` must be one of {{'none', 'data', 'all'}}, got {v}",
                )));
            }
        };
        Ok(Wrap(parsed))
    }
}
