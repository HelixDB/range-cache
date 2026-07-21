#![cfg(feature = "async")]

use std::{
    num::NonZeroUsize,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use range_cache::{
    CacheCapacity, CachedReader, RangeCache, RangeError, RangeReader, ReadError, ReaderConfig,
};
use tokio::{sync::Notify, task::JoinSet};

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("test source failure")]
struct TestError;

struct ActiveRead<'a>(&'a AtomicUsize);

impl Drop for ActiveRead<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct TestSource {
    data: Bytes,
    delay: Duration,
    failures_remaining: AtomicUsize,
    short_reads_remaining: AtomicUsize,
    calls: Mutex<Vec<(String, Range<usize>)>>,
    active: AtomicUsize,
    max_active: AtomicUsize,
    started: Notify,
}

impl TestSource {
    fn new(data: &'static [u8]) -> Self {
        Self {
            data: Bytes::from_static(data),
            delay: Duration::ZERO,
            failures_remaining: AtomicUsize::new(0),
            short_reads_remaining: AtomicUsize::new(0),
            calls: Mutex::new(Vec::new()),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            started: Notify::new(),
        }
    }

    fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    fn with_failures(self, failures: usize) -> Self {
        self.failures_remaining.store(failures, Ordering::SeqCst);
        self
    }

    fn with_short_reads(self, short_reads: usize) -> Self {
        self.short_reads_remaining
            .store(short_reads, Ordering::SeqCst);
        self
    }

    fn calls(&self) -> Vec<(String, Range<usize>)> {
        self.calls
            .lock()
            .expect("calls lock is not poisoned")
            .clone()
    }

    async fn wait_for_calls(&self, expected: usize) {
        loop {
            let notified = self.started.notified();
            if self.calls().len() >= expected {
                return;
            }
            notified.await;
        }
    }

    fn take(counter: &AtomicUsize) -> bool {
        counter
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
    }
}

#[async_trait]
impl RangeReader<String> for TestSource {
    type Error = TestError;

    async fn read_range(&self, key: &String, range: Range<usize>) -> Result<Bytes, Self::Error> {
        self.calls
            .lock()
            .expect("calls lock is not poisoned")
            .push((key.clone(), range.clone()));
        self.started.notify_waiters();
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        let _active_read = ActiveRead(&self.active);

        tokio::time::sleep(self.delay).await;
        if Self::take(&self.failures_remaining) {
            return Err(TestError);
        }

        let response = self.data.slice(range);
        if Self::take(&self.short_reads_remaining) && !response.is_empty() {
            return Ok(response.slice(..response.len() - 1));
        }
        Ok(response)
    }
}

fn reader(
    source: Arc<TestSource>,
    cache: RangeCache<String>,
    max_fetch_concurrency: usize,
) -> CachedReader<String, TestSource> {
    CachedReader::new(
        source,
        cache,
        ReaderConfig::new(
            NonZeroUsize::new(max_fetch_concurrency).expect("test concurrency is non-zero"),
        ),
    )
}

#[tokio::test]
async fn full_hits_skip_the_source_and_partial_hits_fetch_only_gaps() {
    let source = Arc::new(TestSource::new(b"abcdefghijklmnop"));
    let cache = RangeCache::new(CacheCapacity::Unbounded);
    cache
        .insert(String::from("key"), 0..4, Bytes::from_static(b"abcd"))
        .expect("valid insert");
    let reader = reader(Arc::clone(&source), cache, 4);
    let key = String::from("key");

    assert_eq!(
        reader.read(&key, 0..4).await.expect("cached read"),
        Bytes::from_static(b"abcd")
    );
    assert!(source.calls().is_empty());
    assert_eq!(
        reader.read(&key, 0..8).await.expect("partial read"),
        Bytes::from_static(b"abcdefgh")
    );
    assert_eq!(source.calls(), vec![(key.clone(), 4..8)]);
    assert_eq!(
        reader.read(&key, 2..6).await.expect("merged hit"),
        Bytes::from_static(b"cdef")
    );
    assert_eq!(source.calls().len(), 1);
}

#[tokio::test]
async fn missing_gaps_obey_the_global_fetch_concurrency_limit() {
    let source = Arc::new(
        TestSource::new(b"abcdefghijklmnopqrstuvwxyz012345").with_delay(Duration::from_millis(30)),
    );
    let reader = reader(
        Arc::clone(&source),
        RangeCache::new(CacheCapacity::Unbounded),
        2,
    );
    let mut tasks = JoinSet::new();
    for (key, range) in [
        ("first", 0..8),
        ("second", 8..16),
        ("third", 16..24),
        ("fourth", 24..32),
    ] {
        let task_reader = reader.clone();
        let key = String::from(key);
        tasks.spawn(async move { task_reader.read(&key, range).await });
    }
    while let Some(result) = tasks.join_next().await {
        result.expect("task completed").expect("source read");
    }

    assert_eq!(source.calls().len(), 4);
    assert_eq!(source.max_active.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn identical_key_and_gap_requests_are_coalesced() {
    let source =
        Arc::new(TestSource::new(b"abcdefghijklmnop").with_delay(Duration::from_millis(40)));
    let reader = reader(
        Arc::clone(&source),
        RangeCache::new(CacheCapacity::Unbounded),
        8,
    );
    let mut tasks = JoinSet::new();
    for _ in 0..8 {
        let task_reader = reader.clone();
        tasks.spawn(async move {
            task_reader
                .read(&String::from("key"), 0..8)
                .await
                .expect("coalesced read")
        });
    }
    while let Some(result) = tasks.join_next().await {
        assert_eq!(
            result.expect("task completed"),
            Bytes::from_static(b"abcdefgh")
        );
    }
    assert_eq!(source.calls().len(), 1);
}

#[tokio::test]
async fn merely_overlapping_requests_are_not_coalesced() {
    let source =
        Arc::new(TestSource::new(b"abcdefghijklmnop").with_delay(Duration::from_millis(40)));
    let reader = reader(
        Arc::clone(&source),
        RangeCache::new(CacheCapacity::Unbounded),
        2,
    );
    let first_reader = reader.clone();
    let second_reader = reader.clone();
    let first = tokio::spawn(async move {
        first_reader
            .read(&String::from("key"), 0..8)
            .await
            .expect("first read")
    });
    let second = tokio::spawn(async move {
        second_reader
            .read(&String::from("key"), 4..12)
            .await
            .expect("second read")
    });
    assert_eq!(
        first.await.expect("first task"),
        Bytes::from_static(b"abcdefgh")
    );
    assert_eq!(
        second.await.expect("second task"),
        Bytes::from_static(b"efghijkl")
    );
    assert_eq!(source.calls().len(), 2);
}

#[tokio::test]
async fn source_failures_are_not_cached_and_can_be_retried() {
    let source = Arc::new(TestSource::new(b"abcdefgh").with_failures(1));
    let reader = reader(
        Arc::clone(&source),
        RangeCache::new(CacheCapacity::Unbounded),
        1,
    );
    let key = String::from("key");

    assert!(matches!(
        reader.read(&key, 0..8).await,
        Err(ReadError::Source(TestError))
    ));
    assert_eq!(reader.cache().snapshot().resident_bytes, 0);
    assert_eq!(
        reader.read(&key, 0..8).await.expect("retry succeeds"),
        Bytes::from_static(b"abcdefgh")
    );
    assert_eq!(source.calls().len(), 2);
}

#[tokio::test]
async fn short_reads_are_not_cached_and_can_be_retried() {
    let source = Arc::new(TestSource::new(b"abcdefgh").with_short_reads(1));
    let reader = reader(
        Arc::clone(&source),
        RangeCache::new(CacheCapacity::Unbounded),
        1,
    );
    let key = String::from("key");

    assert!(matches!(
        reader.read(&key, 0..8).await,
        Err(ReadError::ShortRead {
            range,
            expected: 8,
            actual: 7,
        }) if range == (0..8)
    ));
    assert_eq!(reader.cache().snapshot().resident_bytes, 0);
    assert_eq!(
        reader.read(&key, 0..8).await.expect("retry succeeds"),
        Bytes::from_static(b"abcdefgh")
    );
    assert_eq!(source.calls().len(), 2);
}

#[tokio::test]
async fn fetched_bytes_are_returned_when_cache_admission_is_too_large() {
    let source = Arc::new(TestSource::new(b"abcdefgh"));
    let cache = RangeCache::new(CacheCapacity::Bounded(
        NonZeroUsize::new(4).expect("non-zero"),
    ));
    let reader = reader(Arc::clone(&source), cache, 1);

    assert_eq!(
        reader
            .read(&String::from("key"), 0..8)
            .await
            .expect("source read"),
        Bytes::from_static(b"abcdefgh")
    );
    let snapshot = reader.cache().snapshot();
    assert_eq!(snapshot.resident_bytes, 0);
    assert_eq!(snapshot.admissions_rejected_too_large, 1);
}

#[tokio::test]
async fn identical_waiters_share_a_successful_response_that_is_too_large_to_cache() {
    let source = Arc::new(TestSource::new(b"abcdefgh").with_delay(Duration::from_millis(40)));
    let cache = RangeCache::new(CacheCapacity::Bounded(
        NonZeroUsize::new(4).expect("non-zero"),
    ));
    let reader = reader(Arc::clone(&source), cache, 2);
    let first_reader = reader.clone();
    let second_reader = reader.clone();
    let first = tokio::spawn(async move {
        first_reader
            .read(&String::from("key"), 0..8)
            .await
            .expect("first response")
    });
    let second = tokio::spawn(async move {
        second_reader
            .read(&String::from("key"), 0..8)
            .await
            .expect("shared response")
    });

    assert_eq!(
        first.await.expect("first task"),
        Bytes::from_static(b"abcdefgh")
    );
    assert_eq!(
        second.await.expect("second task"),
        Bytes::from_static(b"abcdefgh")
    );
    assert_eq!(source.calls().len(), 1);
    assert_eq!(reader.cache().snapshot().resident_bytes, 0);
}

#[tokio::test]
async fn cancelled_leader_allows_an_identical_waiter_to_retry() {
    let source = Arc::new(TestSource::new(b"abcdefgh").with_delay(Duration::from_millis(100)));
    let reader = reader(
        Arc::clone(&source),
        RangeCache::new(CacheCapacity::Unbounded),
        1,
    );
    let leader_reader = reader.clone();
    let leader = tokio::spawn(async move { leader_reader.read(&String::from("key"), 0..8).await });
    source.wait_for_calls(1).await;

    let waiter_reader = reader.clone();
    let waiter = tokio::spawn(async move {
        waiter_reader
            .read(&String::from("key"), 0..8)
            .await
            .expect("waiter retries")
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    leader.abort();
    assert!(leader.await.expect_err("leader cancelled").is_cancelled());
    assert_eq!(
        waiter.await.expect("waiter task"),
        Bytes::from_static(b"abcdefgh")
    );
    assert_eq!(source.calls().len(), 2);
}

#[tokio::test]
async fn cancelled_waiter_does_not_cancel_the_leader() {
    let source = Arc::new(TestSource::new(b"abcdefgh").with_delay(Duration::from_millis(80)));
    let reader = reader(
        Arc::clone(&source),
        RangeCache::new(CacheCapacity::Unbounded),
        1,
    );
    let leader_reader = reader.clone();
    let leader = tokio::spawn(async move {
        leader_reader
            .read(&String::from("key"), 0..8)
            .await
            .expect("leader read")
    });
    source.wait_for_calls(1).await;

    let waiter_reader = reader.clone();
    let waiter = tokio::spawn(async move { waiter_reader.read(&String::from("key"), 0..8).await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    waiter.abort();
    assert!(waiter.await.expect_err("waiter cancelled").is_cancelled());
    assert_eq!(
        leader.await.expect("leader task"),
        Bytes::from_static(b"abcdefgh")
    );
    assert_eq!(source.calls().len(), 1);
}

#[tokio::test]
async fn empty_ranges_succeed_without_source_access_and_reversed_ranges_fail() {
    let source = Arc::new(TestSource::new(b"abcdefgh"));
    let reader = reader(
        Arc::clone(&source),
        RangeCache::new(CacheCapacity::Unbounded),
        1,
    );
    let key = String::from("key");

    assert_eq!(
        reader.read(&key, 4..4).await.expect("empty read"),
        Bytes::new()
    );
    let reversed = Range { start: 5, end: 4 };
    assert!(matches!(
        reader.read(&key, reversed).await,
        Err(ReadError::Range(RangeError::ReversedRange {
            start: 5,
            end: 4
        }))
    ));
    assert!(source.calls().is_empty());
}

#[tokio::test]
async fn range_reader_is_object_safe() {
    let concrete = Arc::new(TestSource::new(b"abcdefgh"));
    let source: Arc<dyn RangeReader<String, Error = TestError>> = concrete;
    let reader = CachedReader::new(
        source,
        RangeCache::new(CacheCapacity::Unbounded),
        ReaderConfig::new(NonZeroUsize::new(1).expect("non-zero")),
    );
    assert_eq!(
        reader
            .read_range(&String::from("key"), 0..4)
            .await
            .expect("trait-object read"),
        Bytes::from_static(b"abcd")
    );
}
