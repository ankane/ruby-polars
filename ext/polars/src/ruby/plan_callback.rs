use magnus::{Value, value::Opaque};
use polars_plan::prelude::{PlanCallback, PlanCallbackArgs, PlanCallbackOut};

pub(crate) trait PlanCallbackExt<Args, Out> {
    fn new_ruby(rbfn: Opaque<Value>) -> Self;
}

impl<Args: PlanCallbackArgs, Out: PlanCallbackOut> PlanCallbackExt<Args, Out>
    for PlanCallback<Args, Out>
{
    fn new_ruby(_rbfn: Opaque<Value>) -> Self {
        todo!();
        // let f = move |_args| {
        //     Ruby::attach(|_rb| {
        //         rb.get_inner(rbfn)
        //             .funcall("call", (args,))
        //             .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
        //     })
        // };
        // Self::Rust(SpecialEq::new(Arc::new(f) as _))
    }
}
