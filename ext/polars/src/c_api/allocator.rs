#[global_allocator]
#[cfg(target_family = "unix")]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[global_allocator]
#[cfg(not(target_family = "unix"))]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;
