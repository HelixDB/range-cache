use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};

use bytes::Bytes;
use range_cache::{CacheCapacity, InsertOutcome, Invalidation, RangeCache, RangeError};

fn bounded(bytes: usize) -> RangeCache<&'static str> {
    RangeCache::new(CacheCapacity::Bounded(
        NonZeroUsize::new(bytes).expect("test capacity is non-zero"),
    ))
}

static KEY_CLONES: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CountedKey(u8);

impl Clone for CountedKey {
    fn clone(&self) -> Self {
        KEY_CLONES.fetch_add(1, Ordering::Relaxed);
        Self(self.0)
    }
}

#[test]
fn cache_is_cloneable_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<RangeCache<String>>();

    let cache = bounded(8);
    let clone = cache.clone();
    cache
        .insert("key", 0..4, Bytes::from_static(b"abcd"))
        .expect("valid insert");
    assert_eq!(
        clone.get(&"key", 0..4).expect("valid range"),
        Some(Bytes::from_static(b"abcd"))
    );
}

#[test]
fn capacity_reports_the_configured_policy() {
    let bounded = CacheCapacity::Bounded(NonZeroUsize::new(8).expect("test capacity is non-zero"));
    assert_eq!(RangeCache::<u8>::new(bounded).capacity(), bounded);
    assert_eq!(
        RangeCache::<u8>::new(CacheCapacity::Unbounded).capacity(),
        CacheCapacity::Unbounded
    );
}

#[test]
fn empty_ranges_succeed_and_matching_empty_inserts_are_noops() {
    let cache = bounded(8);
    assert_eq!(
        cache.get(&"key", 4..4).expect("valid range"),
        Some(Bytes::new())
    );
    assert!(
        cache
            .missing_ranges(&"key", 4..4)
            .expect("valid range")
            .is_empty()
    );
    assert_eq!(
        cache
            .insert("key", 4..4, Bytes::new())
            .expect("valid insert"),
        InsertOutcome::AlreadyCovered
    );
    assert_eq!(cache.snapshot().resident_bytes, 0);
}

#[test]
fn reversed_ranges_and_payload_mismatches_are_errors() {
    let cache = bounded(8);
    let reversed = std::ops::Range { start: 5, end: 4 };
    assert_eq!(
        cache.get(&"key", reversed.clone()),
        Err(RangeError::ReversedRange { start: 5, end: 4 })
    );
    assert_eq!(
        cache.missing_ranges(&"key", reversed),
        Err(RangeError::ReversedRange { start: 5, end: 4 })
    );
    assert_eq!(
        cache.insert("key", 2..5, Bytes::from_static(b"no")),
        Err(RangeError::PayloadLengthMismatch {
            range: 2..5,
            expected: 3,
            actual: 2,
        })
    );
    assert_eq!(
        cache.insert("key", 2..2, Bytes::from_static(b"x")),
        Err(RangeError::PayloadLengthMismatch {
            range: 2..2,
            expected: 0,
            actual: 1,
        })
    );
    assert_eq!(
        cache.insert("key", std::ops::Range { start: 5, end: 4 }, Bytes::new()),
        Err(RangeError::ReversedRange { start: 5, end: 4 })
    );
    assert_eq!(
        cache.insert("key", 2..5, Bytes::from_static(b"long")),
        Err(RangeError::PayloadLengthMismatch {
            range: 2..5,
            expected: 3,
            actual: 4,
        })
    );
    assert_eq!(
        RangeError::ReversedRange { start: 5, end: 4 }.to_string(),
        "reversed byte range 5..4"
    );
}

#[test]
fn ranges_near_usize_max_remain_valid() {
    let cache = RangeCache::new(CacheCapacity::Unbounded);
    let start = usize::MAX - 8;
    cache
        .insert("key", start..usize::MAX, Bytes::from_static(b"abcdefgh"))
        .expect("valid high-offset insert");

    assert_eq!(
        cache
            .get(&"key", start + 2..usize::MAX - 2)
            .expect("valid high-offset read"),
        Some(Bytes::from_static(b"cdef"))
    );
    assert_eq!(
        cache
            .missing_ranges(&"key", start - 2..usize::MAX)
            .expect("valid high-offset range"),
        vec![start - 2..start]
    );
}

#[test]
fn exact_and_subrange_hits_are_zero_copy() {
    let cache = bounded(16);
    let original = Bytes::from_static(b"abcdefgh");
    cache
        .insert("key", 10..18, original.clone())
        .expect("valid insert");

    let exact = cache
        .get(&"key", 10..18)
        .expect("valid range")
        .expect("exact hit");
    let subrange = cache
        .get(&"key", 12..16)
        .expect("valid range")
        .expect("subrange hit");
    assert_eq!(exact, original);
    assert_eq!(subrange, Bytes::from_static(b"cdef"));
    assert_eq!(exact.as_ptr(), original.as_ptr());
    assert_eq!(subrange.as_ptr(), original.slice(2..6).as_ptr());
}

#[test]
fn contained_insert_is_ignored() {
    let cache = bounded(16);
    cache
        .insert("key", 0..8, Bytes::from_static(b"abcdefgh"))
        .expect("valid insert");
    assert_eq!(
        cache
            .insert("key", 2..4, Bytes::from_static(b"XY"))
            .expect("valid insert"),
        InsertOutcome::AlreadyCovered
    );
    assert_eq!(
        cache.get(&"key", 0..8).expect("valid range"),
        Some(Bytes::from_static(b"abcdefgh"))
    );
}

#[test]
fn adjacent_ranges_merge() {
    let cache = bounded(16);
    cache
        .insert("key", 10..14, Bytes::from_static(b"abcd"))
        .expect("valid insert");
    cache
        .insert("key", 14..18, Bytes::from_static(b"efgh"))
        .expect("valid insert");

    assert_eq!(
        cache.get(&"key", 11..17).expect("valid range"),
        Some(Bytes::from_static(b"bcdefg"))
    );
    assert_eq!(cache.snapshot().ranges, 1);
}

#[test]
fn disjoint_ranges_remain_separate_and_report_the_gap() {
    let cache = bounded(16);
    cache
        .insert("key", 0..4, Bytes::from_static(b"abcd"))
        .expect("valid insert");
    cache
        .insert("key", 8..12, Bytes::from_static(b"ijkl"))
        .expect("valid insert");

    assert_eq!(cache.get(&"key", 2..10).expect("valid range"), None);
    assert_eq!(
        cache.missing_ranges(&"key", 2..10).expect("valid range"),
        vec![4..8]
    );
    assert_eq!(cache.snapshot().ranges, 2);
}

#[test]
fn right_overlap_preserves_prefix_and_replaces_overlap() {
    let cache = bounded(16);
    cache
        .insert("key", 0..6, Bytes::from_static(b"abcdef"))
        .expect("valid insert");
    cache
        .insert("key", 4..10, Bytes::from_static(b"EFGHIJ"))
        .expect("valid insert");
    assert_eq!(
        cache.get(&"key", 0..10).expect("valid range"),
        Some(Bytes::from_static(b"abcdEFGHIJ"))
    );
}

#[test]
fn left_overlap_preserves_suffix_and_replaces_overlap() {
    let cache = bounded(16);
    cache
        .insert("key", 6..10, Bytes::from_static(b"ghij"))
        .expect("valid insert");
    cache
        .insert("key", 0..8, Bytes::from_static(b"ABCDEFGH"))
        .expect("valid insert");
    assert_eq!(
        cache.get(&"key", 0..10).expect("valid range"),
        Some(Bytes::from_static(b"ABCDEFGHij"))
    );
}

#[test]
fn bridging_insert_replaces_middle_and_merges_neighbors() {
    let cache = bounded(16);
    cache
        .insert("key", 0..4, Bytes::from_static(b"abcd"))
        .expect("valid insert");
    cache
        .insert("key", 8..12, Bytes::from_static(b"ijkl"))
        .expect("valid insert");
    cache
        .insert("key", 2..10, Bytes::from_static(b"CDEFGHIJ"))
        .expect("valid insert");
    assert_eq!(
        cache.get(&"key", 0..12).expect("valid range"),
        Some(Bytes::from_static(b"abCDEFGHIJkl"))
    );
    assert_eq!(cache.snapshot().ranges, 1);
}

#[test]
fn keys_are_isolated_and_invalidation_is_key_scoped() {
    let cache = RangeCache::new(CacheCapacity::Unbounded);
    cache
        .insert("first", 0..4, Bytes::from_static(b"abcd"))
        .expect("valid insert");
    cache
        .insert("second", 0..4, Bytes::from_static(b"wxyz"))
        .expect("valid insert");

    assert_eq!(cache.get(&"missing", 0..4).expect("valid range"), None);
    assert_eq!(
        cache.invalidate(&"first"),
        Invalidation {
            ranges: 1,
            bytes: 4,
        }
    );
    assert_eq!(cache.get(&"first", 0..4).expect("valid range"), None);
    assert_eq!(
        cache.get(&"second", 0..4).expect("valid range"),
        Some(Bytes::from_static(b"wxyz"))
    );
    assert_eq!(cache.invalidate(&"first"), Invalidation::default());
}

#[test]
fn clear_removes_resident_state_but_retains_statistics() {
    let cache = RangeCache::new(CacheCapacity::Unbounded);
    cache
        .insert("first", 0..4, Bytes::from_static(b"abcd"))
        .expect("valid insert");
    cache
        .insert("second", 4..8, Bytes::from_static(b"efgh"))
        .expect("valid insert");
    cache.get(&"first", 0..4).expect("valid range");

    assert_eq!(
        cache.clear(),
        Invalidation {
            ranges: 2,
            bytes: 8,
        }
    );
    let snapshot = cache.snapshot();
    assert_eq!(snapshot.resident_bytes, 0);
    assert_eq!(snapshot.keys, 0);
    assert_eq!(snapshot.ranges, 0);
    assert_eq!(snapshot.hits, 1);
    assert_eq!(snapshot.insertions, 2);
}

#[test]
fn bounded_cache_never_exceeds_capacity_and_reads_update_lru() {
    let cache = bounded(8);
    cache
        .insert("first", 0..4, Bytes::from_static(b"aaaa"))
        .expect("valid insert");
    cache
        .insert("second", 0..4, Bytes::from_static(b"bbbb"))
        .expect("valid insert");
    cache.get(&"first", 0..4).expect("valid range");
    cache
        .insert("third", 0..4, Bytes::from_static(b"cccc"))
        .expect("valid insert");

    assert_eq!(cache.get(&"second", 0..4).expect("valid range"), None);
    assert_eq!(
        cache.get(&"first", 0..4).expect("valid range"),
        Some(Bytes::from_static(b"aaaa"))
    );
    let snapshot = cache.snapshot();
    assert_eq!(snapshot.resident_bytes, 8);
    assert_eq!(snapshot.ranges, 2);
    assert_eq!(snapshot.evictions, 1);
}

#[test]
fn bounded_partial_hits_touch_each_covered_range() {
    let cache = bounded(8);
    cache
        .insert("first", 0..2, Bytes::from_static(b"ab"))
        .expect("valid insert");
    cache
        .insert("first", 4..6, Bytes::from_static(b"ef"))
        .expect("valid insert");
    cache
        .insert("oldest", 0..4, Bytes::from_static(b"wxyz"))
        .expect("valid insert");

    assert_eq!(cache.get(&"first", 0..6).expect("valid range"), None);
    cache
        .insert("new", 0..2, Bytes::from_static(b"12"))
        .expect("valid insert");

    assert_eq!(cache.get(&"oldest", 0..4).expect("valid range"), None);
    assert_eq!(
        cache.get(&"first", 0..2).expect("valid range"),
        Some(Bytes::from_static(b"ab"))
    );
    assert_eq!(
        cache.get(&"first", 4..6).expect("valid range"),
        Some(Bytes::from_static(b"ef"))
    );
}

#[test]
fn merged_range_is_newer_than_ranges_it_replaces() {
    let cache = bounded(8);
    cache
        .insert("oldest", 0..4, Bytes::from_static(b"wxyz"))
        .expect("valid insert");
    cache
        .insert("merged", 0..2, Bytes::from_static(b"ab"))
        .expect("valid insert");
    cache
        .insert("merged", 4..6, Bytes::from_static(b"ef"))
        .expect("valid insert");
    cache
        .insert("merged", 2..4, Bytes::from_static(b"cd"))
        .expect("valid insert");

    assert_eq!(cache.get(&"oldest", 0..4).expect("valid range"), None);
    assert_eq!(
        cache.get(&"merged", 0..6).expect("valid range"),
        Some(Bytes::from_static(b"abcdef"))
    );
    let snapshot = cache.snapshot();
    assert_eq!(snapshot.resident_bytes, 6);
    assert_eq!(snapshot.ranges, 1);
    assert_eq!(snapshot.evictions, 1);
}

#[test]
fn concurrent_hits_preserve_bytes_and_exact_counters() {
    let cache = Arc::new(RangeCache::new(CacheCapacity::Unbounded));
    cache
        .insert("key", 0..8, Bytes::from_static(b"abcdefgh"))
        .expect("valid insert");
    let workers = if cfg!(miri) { 2 } else { 8 };
    let iterations = if cfg!(miri) { 8 } else { 1_000 };
    let ready = Arc::new(Barrier::new(workers + 1));

    thread::scope(|scope| {
        for _ in 0..workers {
            let cache = Arc::clone(&cache);
            let ready = Arc::clone(&ready);
            scope.spawn(move || {
                ready.wait();
                for _ in 0..iterations {
                    assert_eq!(
                        cache.get(&"key", 0..8).expect("valid range"),
                        Some(Bytes::from_static(b"abcdefgh"))
                    );
                }
            });
        }
        ready.wait();
    });

    let snapshot = cache.snapshot();
    assert_eq!(snapshot.hits, u64::try_from(workers * iterations).unwrap());
    assert_eq!(snapshot.resident_bytes, 8);
    assert_eq!(snapshot.ranges, 1);
}

#[test]
fn bounded_hits_reuse_the_resident_lru_key() {
    let cache = RangeCache::new(CacheCapacity::Bounded(
        NonZeroUsize::new(8).expect("test capacity is non-zero"),
    ));
    let key = CountedKey(1);
    cache
        .insert(key.clone(), 0..4, Bytes::from_static(b"data"))
        .expect("valid insert");
    KEY_CLONES.store(0, Ordering::Relaxed);

    assert_eq!(
        cache.get(&key, 0..4).expect("valid range"),
        Some(Bytes::from_static(b"data"))
    );
    assert_eq!(KEY_CLONES.load(Ordering::Relaxed), 0);
}

#[test]
fn oversized_merge_does_not_mutate_existing_state() {
    let cache = bounded(8);
    cache
        .insert("key", 0..4, Bytes::from_static(b"abcd"))
        .expect("valid insert");
    let before = cache.snapshot();
    assert_eq!(
        cache
            .insert("key", 4..10, Bytes::from_static(b"efghij"))
            .expect("valid insert"),
        InsertOutcome::TooLarge
    );

    assert_eq!(
        cache.get(&"key", 0..4).expect("valid range"),
        Some(Bytes::from_static(b"abcd"))
    );
    assert_eq!(
        cache.missing_ranges(&"key", 0..10).expect("valid range"),
        vec![4..10]
    );
    let after = cache.snapshot();
    assert_eq!(after.resident_bytes, before.resident_bytes);
    assert_eq!(after.ranges, before.ranges);
    assert_eq!(after.insertions, before.insertions);
    assert_eq!(after.admissions_rejected_too_large, 1);
}

#[test]
fn eviction_preserves_range_map_consistency() {
    let cache = bounded(6);
    cache
        .insert("first", 0..3, Bytes::from_static(b"abc"))
        .expect("valid insert");
    cache
        .insert("first", 6..9, Bytes::from_static(b"ghi"))
        .expect("valid insert");
    cache
        .insert("second", 0..3, Bytes::from_static(b"xyz"))
        .expect("valid insert");

    assert_eq!(
        cache.missing_ranges(&"first", 0..9).expect("valid range"),
        vec![0..6]
    );
    assert_eq!(
        cache.get(&"first", 6..9).expect("valid range"),
        Some(Bytes::from_static(b"ghi"))
    );
    assert_eq!(cache.snapshot().resident_bytes, 6);
}

#[test]
fn snapshot_distinguishes_hits_partial_hits_and_misses() {
    let cache = bounded(16);
    cache
        .insert("key", 4..8, Bytes::from_static(b"data"))
        .expect("valid insert");
    cache.get(&"key", 4..8).expect("valid range");
    cache.get(&"key", 2..6).expect("valid range");
    cache.get(&"other", 4..8).expect("valid range");

    let snapshot = cache.snapshot();
    assert_eq!(snapshot.hits, 1);
    assert_eq!(snapshot.partial_hits, 1);
    assert_eq!(snapshot.misses, 1);
}
