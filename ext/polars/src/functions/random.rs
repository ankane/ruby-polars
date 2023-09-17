use crate::RbResult;

pub fn set_random_seed(seed: u64) -> RbResult<()> {
    polars_core::random::set_global_random_seed(seed);
    Ok(())
}
