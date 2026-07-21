//! A thread-safe sparse byte-range cache.
//!
//! The core cache has no async runtime dependency. Enable the `async` feature
//! for a read-through wrapper over arbitrary immutable range sources.

mod cache;
mod error;

#[cfg(feature = "async")]
mod reader;

pub use cache::{CacheCapacity, CacheSnapshot, InsertOutcome, Invalidation, RangeCache};
pub use error::RangeError;

#[cfg(feature = "async")]
pub use reader::{CachedReader, RangeReader, ReadError, ReaderConfig};
