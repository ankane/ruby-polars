use std::sync::Arc;

use magnus::{Ruby, Value, value::ReprValue};
use polars_plan::dsl::SpecialEq;
use polars_plan::prelude::PlanCallback;

use crate::RbResult;
use crate::ruby::gvl::RubyAttach;
use crate::ruby::ruby_function::RubyFunction;
use crate::ruby::thread::{is_non_ruby_thread, run_in_ruby_thread, start_background_ruby_thread};
use crate::ruby::utils::{BoxOpaque, to_pl_err};

pub trait PlanCallbackArgs {
    fn into_rbany(self, rb: &Ruby) -> RbResult<Value>;
}

pub trait PlanCallbackOut: Sized {
    fn from_rbany(rbany: Value, rb: &Ruby) -> RbResult<Self>;
}

mod _ruby {
    use std::sync::Arc;

    use crate::RbResult;
    use magnus::{IntoValue, RArray, Ruby, TryConvert, Value, value::ReprValue};
    use polars_utils::pl_str::PlSmallStr;

    macro_rules! impl_rbcb_type {
        ($($type:ty),+) => {
            $(
            impl super::PlanCallbackArgs for $type {
                fn into_rbany(self, rb: &Ruby) -> RbResult<Value> {
                    Ok(self.into_value_with(rb))
                }
            }

            impl super::PlanCallbackOut for $type {
                fn from_rbany(rbany: Value, _rb: &Ruby) -> RbResult<Self> {
                    Self::try_convert(rbany)
                }
            }
            )+
        };
    }

    macro_rules! impl_rbcb_type_to_from {
        ($($type:ty => $transformed:ty),+) => {
            $(
            impl super::PlanCallbackArgs for $type {
                fn into_rbany(self, rb: &Ruby) -> RbResult<Value> {
                    Ok(<$transformed>::from(self).into_value_with(rb))
                }
            }

            impl super::PlanCallbackOut for $type {
                fn from_rbany(rbany: Value, _rb: &Ruby) -> RbResult<Self> {
                    <$transformed>::try_convert(rbany).map(Into::into)
                }
            }
            )+
        };
    }

    macro_rules! impl_registrycb_type {
        ($(($type:path, $from:ident, $to:ident)),+) => {
            $(
            impl super::PlanCallbackArgs for $type {
                fn into_rbany(self, _rb: &Ruby) -> RbResult<Value> {
                    let registry = crate::ruby::ruby_convert_registry::get_ruby_convert_registry();
                    (registry.to_rb.$to)(&self)
                }
            }

            impl super::PlanCallbackOut for $type {
                fn from_rbany(rbany: Value, _rb: &Ruby) -> RbResult<Self> {
                    let registry = crate::ruby::ruby_convert_registry::get_ruby_convert_registry();
                    // TODO remove into
                    let obj = (registry.from_rb.$from)(rbany.into())?;
                    let obj = obj.downcast().unwrap();
                    Ok(*obj)
                }
            }
            )+
        };
    }

    impl<T: super::PlanCallbackArgs> super::PlanCallbackArgs for Option<T> {
        fn into_rbany(self, rb: &Ruby) -> RbResult<Value> {
            match self {
                None => Ok(rb.qnil().as_value()),
                Some(v) => v.into_rbany(rb),
            }
        }
    }

    impl<T: super::PlanCallbackOut> super::PlanCallbackOut for Option<T> {
        fn from_rbany(rbany: Value, rb: &Ruby) -> RbResult<Self> {
            if rbany.is_nil() {
                Ok(None)
            } else {
                T::from_rbany(rbany, rb).map(Some)
            }
        }
    }

    impl<T, U> super::PlanCallbackArgs for (T, U)
    where
        T: super::PlanCallbackArgs,
        U: super::PlanCallbackArgs,
    {
        fn into_rbany(self, rb: &Ruby) -> RbResult<Value> {
            Ok(rb
                .ary_new_from_values(&[self.0.into_rbany(rb)?, self.1.into_rbany(rb)?])
                .as_value())
        }
    }

    impl<T, U> super::PlanCallbackOut for (T, U)
    where
        T: super::PlanCallbackOut,
        U: super::PlanCallbackOut,
    {
        fn from_rbany(rbany: Value, rb: &Ruby) -> RbResult<Self> {
            // TODO remove unwrap
            let tuple = RArray::try_convert(rbany)?;
            Ok((
                T::from_rbany(tuple.entry(0)?, rb)?,
                U::from_rbany(tuple.entry(1)?, rb)?,
            ))
        }
    }

    impl_rbcb_type! {
        bool,
        usize,
        String
    }
    impl_rbcb_type_to_from! {
        PlSmallStr => String
    }
    impl_registrycb_type! {
        (polars_core::series::Series, series, series),
        (polars_core::frame::DataFrame, df, df),
        (polars_plan::dsl::DslPlan, dsl_plan, dsl_plan),
        (polars_core::schema::Schema, schema, schema)
    }

    impl<T: super::PlanCallbackArgs + Clone> super::PlanCallbackArgs for Arc<T> {
        fn into_rbany(self, rb: &Ruby) -> RbResult<Value> {
            Arc::unwrap_or_clone(self).into_rbany(rb)
        }
    }

    impl<T: super::PlanCallbackArgs + Clone> super::PlanCallbackArgs for Vec<T> {
        fn into_rbany(self, rb: &Ruby) -> RbResult<Value> {
            rb.ary_try_from_iter(self.into_iter().map(|v| v.into_rbany(rb)))
                .map(|v| v.as_value())
        }
    }

    impl<T: super::PlanCallbackOut> super::PlanCallbackOut for Arc<T> {
        fn from_rbany(rbany: Value, rb: &Ruby) -> RbResult<Self> {
            T::from_rbany(rbany, rb).map(Arc::from)
        }
    }
}

pub(crate) trait PlanCallbackExt<Args, Out> {
    fn new_ruby(rbfn: RubyFunction) -> Self;
}

impl<Args: PlanCallbackArgs + Send + 'static, Out: PlanCallbackOut + Send + 'static>
    PlanCallbackExt<Args, Out> for PlanCallback<Args, Out>
{
    fn new_ruby(rbfn: RubyFunction) -> Self {
        let boxed = BoxOpaque::new(rbfn.0);

        start_background_ruby_thread(&Ruby::get().unwrap());

        let f = move |args: Args, boxed: &BoxOpaque| {
            Ruby::attach(|rb| {
                let out = Out::from_rbany(
                    rb.get_inner(*boxed.0)
                        .funcall("call", (args.into_rbany(rb).map_err(to_pl_err)?,))
                        .map_err(to_pl_err)?,
                    rb,
                )
                .map_err(to_pl_err)?;
                Ok(out)
            })
        };
        let f = move |args: Args| {
            if is_non_ruby_thread() {
                let boxed = boxed.clone();
                return run_in_ruby_thread(move |_rb| f(args, &boxed));
            }

            f(args, &boxed)
        };
        Self::Rust(SpecialEq::new(Arc::new(f) as _))
    }
}
