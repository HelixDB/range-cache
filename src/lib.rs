#![doc = include_str!("../README.md")]

mod cache;
mod error;

#[cfg(feature = "async")]
mod reader;

pub use cache::{CacheCapacity, CacheSnapshot, InsertOutcome, Invalidation, RangeCache};
pub use error::RangeError;

#[cfg(feature = "async")]
pub use reader::{CachedReader, RangeReader, ReadError, ReaderConfig};
