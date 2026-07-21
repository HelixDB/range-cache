//! Core cache types.

use std::{marker::PhantomData, num::NonZeroUsize, sync::Arc};

use parking_lot::Mutex;

/// The maximum number of payload bytes resident in a cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheCapacity {
    /// Evict least-recently-used ranges to enforce the given byte ceiling.
    Bounded(NonZeroUsize),
    /// Retain all admitted ranges until explicit invalidation.
    Unbounded,
}

/// The result of an insertion attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InsertOutcome {
    /// The bytes were admitted to the cache.
    Inserted,
    /// Existing coverage already fully contained the inserted range.
    AlreadyCovered,
    /// The resulting merged range exceeded the bounded capacity.
    TooLarge,
}

/// The effect of invalidating cache state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Invalidation {
    /// Number of ranges removed.
    pub ranges: usize,
    /// Number of payload bytes removed.
    pub bytes: usize,
}

/// A point-in-time cache state and statistics snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheSnapshot {
    /// Configured capacity.
    pub capacity: CacheCapacity,
    /// Total resident payload bytes.
    pub resident_bytes: usize,
    /// Number of keys with resident ranges.
    pub keys: usize,
    /// Number of resident ranges.
    pub ranges: usize,
    /// Fully cached reads.
    pub hits: u64,
    /// Reads with some, but not all, requested bytes cached.
    pub partial_hits: u64,
    /// Reads with none of the requested bytes cached.
    pub misses: u64,
    /// Successfully admitted insertions.
    pub insertions: u64,
    /// Insertions rejected because the merged range was too large.
    pub admissions_rejected_too_large: u64,
    /// Ranges removed by capacity enforcement.
    pub evictions: u64,
}

/// A cloneable, thread-safe sparse cache of byte ranges keyed by `K`.
pub struct RangeCache<K> {
    pub(crate) inner: Arc<Mutex<()>>,
    capacity: CacheCapacity,
    key: PhantomData<fn() -> K>,
}

impl<K> Clone for RangeCache<K> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            capacity: self.capacity,
            key: PhantomData,
        }
    }
}

impl<K> RangeCache<K> {
    /// Creates an empty cache with an explicit capacity policy.
    #[must_use]
    pub fn new(capacity: CacheCapacity) -> Self {
        Self {
            inner: Arc::new(Mutex::new(())),
            capacity,
            key: PhantomData,
        }
    }

    /// Returns the configured capacity policy.
    #[must_use]
    pub const fn capacity(&self) -> CacheCapacity {
        self.capacity
    }
}
