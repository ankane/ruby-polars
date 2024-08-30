#[cfg(target_os = "linux")]
use jemallocator::Jemalloc;

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
use mimalloc::MiMalloc;

#[global_allocator]
#[cfg(target_os = "linux")]
static ALLOC: Jemalloc = Jemalloc;

#[global_allocator]
#[cfg(not(any(target_os = "linux", target_os = "windows")))]
static ALLOC: MiMalloc = MiMalloc;
