#![cfg(feature = "async")]

use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
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
    failures_remaining: AtomicUsize,
    short_reads_remaining: AtomicUsize,
    long_reads_remaining: AtomicUsize,
    failures_by_range: Mutex<Vec<(String, Range<usize>)>>,
    gates: Mutex<BTreeMap<(String, usize, usize), Arc<tokio::sync::Semaphore>>>,
    calls: Mutex<Vec<(String, Range<usize>)>>,
    active: AtomicUsize,
    max_active: AtomicUsize,
    started: Notify,
}

impl TestSource {
    fn new(data: &'static [u8]) -> Self {
        Self {
            data: Bytes::from_static(data),
            failures_remaining: AtomicUsize::new(0),
            short_reads_remaining: AtomicUsize::new(0),
            long_reads_remaining: AtomicUsize::new(0),
            failures_by_range: Mutex::new(Vec::new()),
            gates: Mutex::new(BTreeMap::new()),
            calls: Mutex::new(Vec::new()),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            started: Notify::new(),
        }
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

    fn with_long_reads(self, long_reads: usize) -> Self {
        self.long_reads_remaining
            .store(long_reads, Ordering::SeqCst);
        self
    }

    fn with_failure_for(self, key: &str, range: Range<usize>) -> Self {
        self.failures_by_range
            .lock()
            .expect("failure script lock is not poisoned")
            .push((String::from(key), range));
        self
    }

    fn with_gate(self, key: &str, range: Range<usize>) -> Self {
        self.gates
            .lock()
            .expect("gate lock is not poisoned")
            .insert(
                (String::from(key), range.start, range.end),
                Arc::new(tokio::sync::Semaphore::new(0)),
            );
        self
    }

    fn release(&self, key: &str, range: Range<usize>) {
        self.gates
            .lock()
            .expect("gate lock is not poisoned")
            .get(&(String::from(key), range.start, range.end))
            .expect("scripted source gate exists")
            .add_permits(1);
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

        let gate = self
            .gates
            .lock()
            .expect("gate lock is not poisoned")
            .get(&(key.clone(), range.start, range.end))
            .cloned();
        if let Some(gate) = gate {
            gate.acquire()
                .await
                .expect("source gate remains open")
                .forget();
        }
        if Self::take(&self.failures_remaining) {
            return Err(TestError);
        }
        let mut failures_by_range = self
            .failures_by_range
            .lock()
            .expect("failure script lock is not poisoned");
        if let Some(index) = failures_by_range
            .iter()
            .position(|failure| failure == &(key.clone(), range.clone()))
        {
            failures_by_range.remove(index);
            return Err(TestError);
        }
        drop(failures_by_range);

        let response = self.data.slice(range);
        if Self::take(&self.short_reads_remaining) && !response.is_empty() {
            return Ok(response.slice(..response.len() - 1));
        }
        if Self::take(&self.long_reads_remaining) {
            let mut longer = response.to_vec();
            longer.push(0);
            return Ok(Bytes::from(longer));
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
async fn one_gap_partial_reads_insert_the_fetched_chunk_in_order() {
    let source = Arc::new(TestSource::new(b"abcdefghijklmnop"));
    let cache = RangeCache::new(CacheCapacity::Unbounded);
    cache
        .insert(String::from("prefix"), 4..8, Bytes::from_static(b"efgh"))
        .expect("valid insert");
    cache
        .insert(String::from("middle"), 0..2, Bytes::from_static(b"ab"))
        .expect("valid insert");
    cache
        .insert(String::from("middle"), 6..8, Bytes::from_static(b"gh"))
        .expect("valid insert");
    let reader = reader(Arc::clone(&source), cache, 2);

    assert_eq!(
        reader
            .read(&String::from("prefix"), 0..8)
            .await
            .expect("prefix gap read"),
        Bytes::from_static(b"abcdefgh")
    );
    assert_eq!(
        reader
            .read(&String::from("middle"), 0..8)
            .await
            .expect("middle gap read"),
        Bytes::from_static(b"abcdefgh")
    );
    assert_eq!(
        source.calls(),
        vec![
            (String::from("prefix"), 0..4),
            (String::from("middle"), 2..6),
        ]
    );
}

#[tokio::test]
async fn multiple_gaps_complete_out_of_order_and_reconstruct_exactly() {
    let source = Arc::new(
        TestSource::new(b"abcdefghijkl")
            .with_gate("key", 2..4)
            .with_gate("key", 6..8),
    );
    let cache = RangeCache::new(CacheCapacity::Unbounded);
    for (range, bytes) in [
        (0..2, Bytes::from_static(b"ab")),
        (4..6, Bytes::from_static(b"ef")),
        (8..12, Bytes::from_static(b"ijkl")),
    ] {
        cache
            .insert(String::from("key"), range, bytes)
            .expect("valid insert");
    }
    let reader = reader(Arc::clone(&source), cache, 2);
    let task_reader = reader.clone();
    let task = tokio::spawn(async move { task_reader.read(&String::from("key"), 0..12).await });

    source.wait_for_calls(2).await;
    assert_eq!(source.max_active.load(Ordering::SeqCst), 2);
    source.release("key", 6..8);
    tokio::task::yield_now().await;
    source.release("key", 2..4);

    assert_eq!(
        task.await
            .expect("task completed")
            .expect("multi-gap read succeeds"),
        Bytes::from_static(b"abcdefghijkl")
    );
    assert_eq!(
        source.calls(),
        vec![(String::from("key"), 2..4), (String::from("key"), 6..8),]
    );
}

#[tokio::test]
async fn completed_gaps_remain_cached_when_another_gap_fails() {
    let source = Arc::new(TestSource::new(b"abcdefghij").with_failure_for("key", 6..8));
    let cache = RangeCache::new(CacheCapacity::Unbounded);
    for (range, bytes) in [
        (0..2, Bytes::from_static(b"ab")),
        (4..6, Bytes::from_static(b"ef")),
        (8..10, Bytes::from_static(b"ij")),
    ] {
        cache
            .insert(String::from("key"), range, bytes)
            .expect("valid insert");
    }
    let reader = reader(Arc::clone(&source), cache, 2);
    let key = String::from("key");

    assert!(matches!(
        reader.read(&key, 0..10).await,
        Err(ReadError::Source(TestError))
    ));
    assert_eq!(
        reader
            .cache()
            .missing_ranges(&key, 0..10)
            .expect("valid range"),
        vec![6..8]
    );
    assert_eq!(
        reader.read(&key, 0..10).await.expect("retry succeeds"),
        Bytes::from_static(b"abcdefghij")
    );
    assert_eq!(
        source.calls(),
        vec![
            (String::from("key"), 2..4),
            (String::from("key"), 6..8),
            (String::from("key"), 6..8),
        ]
    );
}

#[tokio::test]
async fn missing_gaps_obey_the_global_fetch_concurrency_limit() {
    let source = Arc::new(
        TestSource::new(b"abcdefghijklmnopqrstuvwxyz012345")
            .with_gate("first", 0..8)
            .with_gate("second", 8..16)
            .with_gate("third", 16..24)
            .with_gate("fourth", 24..32),
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
    source.wait_for_calls(2).await;
    assert_eq!(source.max_active.load(Ordering::SeqCst), 2);
    for (key, range) in source.calls() {
        source.release(&key, range);
    }
    source.wait_for_calls(4).await;
    for (key, range) in source.calls().into_iter().skip(2) {
        source.release(&key, range);
    }
    while let Some(result) = tasks.join_next().await {
        result.expect("task completed").expect("source read");
    }

    assert_eq!(source.calls().len(), 4);
    assert_eq!(source.max_active.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn identical_key_and_gap_requests_are_coalesced() {
    let source = Arc::new(TestSource::new(b"abcdefghijklmnop").with_gate("key", 0..8));
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
    source.wait_for_calls(1).await;
    source.release("key", 0..8);
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
    let source = Arc::new(
        TestSource::new(b"abcdefghijklmnop")
            .with_gate("key", 0..8)
            .with_gate("key", 4..12),
    );
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
    source.wait_for_calls(2).await;
    source.release("key", 0..8);
    source.release("key", 4..12);
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
async fn identical_ranges_for_different_keys_are_not_coalesced() {
    let source = Arc::new(
        TestSource::new(b"abcdefgh")
            .with_gate("first", 0..8)
            .with_gate("second", 0..8),
    );
    let reader = reader(
        Arc::clone(&source),
        RangeCache::new(CacheCapacity::Unbounded),
        2,
    );
    let first_reader = reader.clone();
    let second_reader = reader.clone();
    let first = tokio::spawn(async move { first_reader.read(&String::from("first"), 0..8).await });
    let second =
        tokio::spawn(async move { second_reader.read(&String::from("second"), 0..8).await });

    source.wait_for_calls(2).await;
    source.release("first", 0..8);
    source.release("second", 0..8);
    assert_eq!(
        first.await.expect("first task").expect("first read"),
        Bytes::from_static(b"abcdefgh")
    );
    assert_eq!(
        second.await.expect("second task").expect("second read"),
        Bytes::from_static(b"abcdefgh")
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
async fn failed_coalesced_leader_allows_a_waiter_to_retry() {
    let source = Arc::new(
        TestSource::new(b"abcdefgh")
            .with_failures(1)
            .with_gate("key", 0..8),
    );
    let reader = reader(
        Arc::clone(&source),
        RangeCache::new(CacheCapacity::Unbounded),
        1,
    );
    let first_reader = reader.clone();
    let second_reader = reader.clone();
    let first = tokio::spawn(async move { first_reader.read(&String::from("key"), 0..8).await });
    let second = tokio::spawn(async move { second_reader.read(&String::from("key"), 0..8).await });

    source.wait_for_calls(1).await;
    source.release("key", 0..8);
    source.wait_for_calls(2).await;
    source.release("key", 0..8);
    let results = [
        first.await.expect("first task"),
        second.await.expect("second task"),
    ];
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(ReadError::Source(TestError))))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| {
                matches!(result, Ok(bytes) if bytes == &Bytes::from_static(b"abcdefgh"))
            })
            .count(),
        1
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
async fn long_reads_are_not_cached_and_can_be_retried() {
    let source = Arc::new(TestSource::new(b"abcdefgh").with_long_reads(1));
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
            actual: 9,
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
    let source = Arc::new(TestSource::new(b"abcdefgh").with_gate("key", 0..8));
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
    source.wait_for_calls(1).await;
    source.release("key", 0..8);

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
    let source = Arc::new(TestSource::new(b"abcdefgh").with_gate("key", 0..8));
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
    tokio::task::yield_now().await;
    leader.abort();
    assert!(leader.await.expect_err("leader cancelled").is_cancelled());
    source.wait_for_calls(2).await;
    source.release("key", 0..8);
    assert_eq!(
        waiter.await.expect("waiter task"),
        Bytes::from_static(b"abcdefgh")
    );
    assert_eq!(source.calls().len(), 2);
}

#[tokio::test]
async fn cancelled_waiter_does_not_cancel_the_leader() {
    let source = Arc::new(TestSource::new(b"abcdefgh").with_gate("key", 0..8));
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
    tokio::task::yield_now().await;
    waiter.abort();
    assert!(waiter.await.expect_err("waiter cancelled").is_cancelled());
    source.release("key", 0..8);
    assert_eq!(
        leader.await.expect("leader task"),
        Bytes::from_static(b"abcdefgh")
    );
    assert_eq!(source.calls().len(), 1);
}

#[tokio::test]
async fn cancelling_a_fetch_permit_waiter_does_not_consume_a_permit() {
    let source = Arc::new(
        TestSource::new(b"abcdefgh")
            .with_gate("first", 0..4)
            .with_gate("second", 4..8),
    );
    let reader = reader(
        Arc::clone(&source),
        RangeCache::new(CacheCapacity::Unbounded),
        1,
    );
    let first_reader = reader.clone();
    let first = tokio::spawn(async move { first_reader.read(&String::from("first"), 0..4).await });
    source.wait_for_calls(1).await;

    let cancelled_reader = reader.clone();
    let cancelled =
        tokio::spawn(async move { cancelled_reader.read(&String::from("second"), 4..8).await });
    tokio::task::yield_now().await;
    assert_eq!(source.calls().len(), 1);
    cancelled.abort();
    assert!(
        cancelled
            .await
            .expect_err("waiting task was cancelled")
            .is_cancelled()
    );

    source.release("first", 0..4);
    assert_eq!(
        first.await.expect("first task").expect("first read"),
        Bytes::from_static(b"abcd")
    );

    let retry_reader = reader.clone();
    let retry = tokio::spawn(async move { retry_reader.read(&String::from("second"), 4..8).await });
    source.wait_for_calls(2).await;
    source.release("second", 4..8);
    assert_eq!(
        retry.await.expect("retry task").expect("retry read"),
        Bytes::from_static(b"efgh")
    );
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

#[test]
fn reader_accessors_return_constructor_values() {
    let source = Arc::new(TestSource::new(b"abcdefgh"));
    let capacity = CacheCapacity::Bounded(NonZeroUsize::new(8).expect("test capacity is non-zero"));
    let config = ReaderConfig::new(NonZeroUsize::new(3).expect("test concurrency is non-zero"));
    let reader = CachedReader::new(
        Arc::clone(&source),
        RangeCache::<String>::new(capacity),
        config,
    );

    assert!(Arc::ptr_eq(reader.source(), &source));
    assert_eq!(reader.cache().capacity(), capacity);
    assert_eq!(reader.config(), config);
    assert_eq!(config.max_fetch_concurrency().get(), 3);
}
