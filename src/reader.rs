//! Optional async read-through types.

use std::{num::NonZeroUsize, ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;

use crate::{RangeCache, RangeError};

/// An immutable source capable of reading exact byte ranges.
#[async_trait]
pub trait RangeReader<K>: Send + Sync {
    /// Source-specific error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Reads the requested byte range for `key`.
    async fn read_range(&self, key: &K, range: Range<usize>) -> Result<Bytes, Self::Error>;
}

/// Configuration for [`CachedReader`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReaderConfig {
    max_fetch_concurrency: NonZeroUsize,
}

impl ReaderConfig {
    /// Creates a configuration with an explicitly non-zero concurrency limit.
    #[must_use]
    pub const fn new(max_fetch_concurrency: NonZeroUsize) -> Self {
        Self {
            max_fetch_concurrency,
        }
    }

    /// Returns the maximum number of source reads started by one cache read.
    #[must_use]
    pub const fn max_fetch_concurrency(self) -> NonZeroUsize {
        self.max_fetch_concurrency
    }
}

/// A read-through failure.
#[derive(Debug, thiserror::Error)]
pub enum ReadError<E> {
    /// Invalid range or cache payload.
    #[error(transparent)]
    Range(#[from] RangeError),
    /// Source-specific failure.
    #[error("range source failed: {0}")]
    Source(#[source] E),
    /// The source returned fewer or more bytes than requested.
    #[error("source returned {actual} bytes for {range:?}; expected {expected}")]
    ShortRead {
        /// Requested byte range.
        range: Range<usize>,
        /// Required byte length.
        expected: usize,
        /// Actual byte length.
        actual: usize,
    },
}

/// An async read-through adapter over a [`RangeReader`].
pub struct CachedReader<K, R: ?Sized> {
    source: Arc<R>,
    cache: RangeCache<K>,
    config: ReaderConfig,
}

impl<K, R: ?Sized> CachedReader<K, R> {
    /// Wraps `source` with the provided cache and concurrency policy.
    #[must_use]
    pub fn new(source: Arc<R>, cache: RangeCache<K>, config: ReaderConfig) -> Self {
        Self {
            source,
            cache,
            config,
        }
    }

    /// Returns the shared cache.
    #[must_use]
    pub const fn cache(&self) -> &RangeCache<K> {
        &self.cache
    }

    /// Returns the wrapped source.
    #[must_use]
    pub const fn source(&self) -> &Arc<R> {
        &self.source
    }

    /// Returns the reader configuration.
    #[must_use]
    pub const fn config(&self) -> ReaderConfig {
        self.config
    }
}
