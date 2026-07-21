//! Optional async read-through types.

use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    ops::Range,
    sync::{Arc, Weak},
};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, TryStreamExt, stream};
use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

use crate::{RangeCache, RangeError, cache::ReadPlan};

/// An immutable source capable of reading byte ranges.
///
/// Implementations must return exactly `range.len()` bytes on success. A
/// [`CachedReader`] checks this contract before admitting a response.
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

    /// Returns the global maximum number of concurrent source fetches.
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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct InFlightKey<K> {
    key: K,
    start: usize,
    end: usize,
}

struct InFlightRegistry<K: Ord> {
    entries: Mutex<BTreeMap<InFlightKey<K>, Weak<InFlightEntry>>>,
}

impl<K: Ord> Default for InFlightRegistry<K> {
    fn default() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
        }
    }
}

struct InFlightEntry {
    gate: AsyncMutex<()>,
    successful_response: Mutex<Option<Bytes>>,
}

impl<K: Ord + Clone> InFlightRegistry<K> {
    fn register(self: &Arc<Self>, request: InFlightKey<K>) -> InFlightRegistration<K> {
        let entry = {
            let mut entries = self.entries.lock();
            let current = entries.get(&request).and_then(Weak::upgrade);
            let entry = current.unwrap_or_else(|| {
                Arc::new(InFlightEntry {
                    gate: AsyncMutex::new(()),
                    successful_response: Mutex::new(None),
                })
            });
            entries.insert(request.clone(), Arc::downgrade(&entry));
            entry
        };
        InFlightRegistration {
            registry: Arc::clone(self),
            request,
            entry,
        }
    }
}

struct InFlightRegistration<K: Ord> {
    registry: Arc<InFlightRegistry<K>>,
    request: InFlightKey<K>,
    entry: Arc<InFlightEntry>,
}

impl<K: Ord> Drop for InFlightRegistration<K> {
    fn drop(&mut self) {
        let mut entries = self.registry.entries.lock();
        let registered_is_self = entries
            .get(&self.request)
            .is_some_and(|registered| registered.ptr_eq(&Arc::downgrade(&self.entry)));
        if registered_is_self && Arc::strong_count(&self.entry) == 1 {
            entries.remove(&self.request);
        }
    }
}

/// An async read-through adapter over a [`RangeReader`].
pub struct CachedReader<K: Ord, R: ?Sized> {
    source: Arc<R>,
    cache: RangeCache<K>,
    config: ReaderConfig,
    fetch_limit: Arc<Semaphore>,
    in_flight: Arc<InFlightRegistry<K>>,
}

impl<K: Ord, R: ?Sized> Clone for CachedReader<K, R> {
    fn clone(&self) -> Self {
        Self {
            source: Arc::clone(&self.source),
            cache: self.cache.clone(),
            config: self.config,
            fetch_limit: Arc::clone(&self.fetch_limit),
            in_flight: Arc::clone(&self.in_flight),
        }
    }
}

impl<K: Ord, R: ?Sized> CachedReader<K, R> {
    /// Wraps `source` with the provided cache and concurrency policy.
    #[must_use]
    pub fn new(source: Arc<R>, cache: RangeCache<K>, config: ReaderConfig) -> Self {
        Self {
            source,
            cache,
            config,
            fetch_limit: Arc::new(Semaphore::new(config.max_fetch_concurrency.get())),
            in_flight: Arc::new(InFlightRegistry::default()),
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

impl<K, R> CachedReader<K, R>
where
    K: Ord + Clone,
    R: RangeReader<K> + ?Sized,
{
    /// Reads a range, fetching and caching only its missing gaps.
    ///
    /// Identical in-flight key-and-gap requests share one source fetch. Other
    /// overlapping requests remain independent. Responses rejected by the
    /// cache capacity policy are still returned to the caller.
    ///
    /// # Errors
    ///
    /// Returns a validation error, source error, or [`ReadError::ShortRead`].
    /// Source failures and invalid response lengths are never cached.
    ///
    /// # Panics
    ///
    /// Panics only if the privately owned fetch semaphore is unexpectedly
    /// closed or an internal read-plan coverage invariant is violated.
    pub async fn read(&self, key: &K, range: Range<usize>) -> Result<Bytes, ReadError<R::Error>> {
        let (mut cached, missing) = match self.cache.read_plan(key, range.clone())? {
            ReadPlan::Complete(bytes) => return Ok(bytes),
            ReadPlan::Fetch { cached, missing } => (cached, missing),
        };

        let fetched = stream::iter(missing)
            .map(|gap| self.fetch_gap(key, gap))
            .buffer_unordered(self.config.max_fetch_concurrency.get())
            .try_collect::<Vec<_>>()
            .await?;
        cached.extend(fetched);
        cached.sort_unstable_by_key(|(chunk_range, _)| chunk_range.start);

        if cached.len() == 1 && cached[0].0 == range {
            return Ok(cached.pop().expect("single chunk exists").1);
        }

        let mut reconstructed = BytesMut::with_capacity(range.len());
        let mut cursor = range.start;
        for (chunk_range, bytes) in cached {
            assert_eq!(chunk_range.start, cursor, "read plan has no gaps");
            assert_eq!(chunk_range.len(), bytes.len(), "chunk length is exact");
            reconstructed.extend_from_slice(&bytes);
            cursor = chunk_range.end;
        }
        assert_eq!(cursor, range.end, "read plan covers the request");
        Ok(reconstructed.freeze())
    }

    async fn fetch_gap(
        &self,
        key: &K,
        range: Range<usize>,
    ) -> Result<(Range<usize>, Bytes), ReadError<R::Error>> {
        let registration = self.in_flight.register(InFlightKey {
            key: key.clone(),
            start: range.start,
            end: range.end,
        });
        let _request_guard = registration.entry.gate.lock().await;

        if let Some(bytes) = registration.entry.successful_response.lock().clone() {
            return Ok((range, bytes));
        }

        if let Some(bytes) = self.cache.get(key, range.clone())? {
            return Ok((range, bytes));
        }

        let _fetch_permit = self
            .fetch_limit
            .acquire()
            .await
            .expect("private fetch semaphore remains open");
        let bytes = self
            .source
            .read_range(key, range.clone())
            .await
            .map_err(ReadError::Source)?;
        let expected = range.len();
        if bytes.len() != expected {
            return Err(ReadError::ShortRead {
                range,
                expected,
                actual: bytes.len(),
            });
        }

        let _ = self
            .cache
            .insert(key.clone(), range.clone(), bytes.clone())?;
        *registration.entry.successful_response.lock() = Some(bytes.clone());
        Ok((range, bytes))
    }
}

#[async_trait]
impl<K, R> RangeReader<K> for CachedReader<K, R>
where
    K: Ord + Clone + Send + Sync,
    R: RangeReader<K> + ?Sized,
{
    type Error = ReadError<R::Error>;

    async fn read_range(&self, key: &K, range: Range<usize>) -> Result<Bytes, Self::Error> {
        self.read(key, range).await
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, num::NonZeroUsize, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use bytes::Bytes;
    use tokio::sync::Semaphore;

    use super::{CachedReader, RangeReader, ReaderConfig};
    use crate::{CacheCapacity, RangeCache};

    struct BlockingSource {
        started: Semaphore,
    }

    #[async_trait]
    impl RangeReader<String> for BlockingSource {
        type Error = Infallible;

        async fn read_range(
            &self,
            _key: &String,
            range: std::ops::Range<usize>,
        ) -> Result<Bytes, Self::Error> {
            self.started.add_permits(1);
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(Bytes::from(vec![0; range.len()]))
        }
    }

    #[tokio::test]
    async fn cancelled_leader_removes_its_in_flight_registration() {
        let source = Arc::new(BlockingSource {
            started: Semaphore::new(0),
        });
        let reader = CachedReader::new(
            Arc::clone(&source),
            RangeCache::new(CacheCapacity::Unbounded),
            ReaderConfig::new(NonZeroUsize::new(1).expect("non-zero")),
        );
        let task_reader = reader.clone();
        let task = tokio::spawn(async move {
            let key = String::from("key");
            task_reader.read(&key, 0..4).await
        });
        source
            .started
            .acquire()
            .await
            .expect("source semaphore remains open")
            .forget();
        task.abort();
        assert!(task.await.expect_err("task was cancelled").is_cancelled());

        tokio::task::yield_now().await;
        assert!(reader.in_flight.entries.lock().is_empty());
    }
}
