use std::num::NonZeroUsize;

use bytes::Bytes;
use proptest::prelude::*;
use range_cache::{CacheCapacity, InsertOutcome, Invalidation, RangeCache};

const KEY_COUNT: usize = 3;
const SOURCE_LENGTH: usize = 32;

#[derive(Clone, Copy, Debug)]
enum Operation {
    Insert,
    Get,
    Missing,
    Invalidate,
}

#[derive(Clone, Copy, Debug)]
enum BoundedOperation {
    Insert,
    Get,
    Invalidate,
    Clear,
}

fn operation() -> impl Strategy<Value = (Operation, usize, usize, usize, u8)> {
    (
        prop_oneof![
            4 => Just(Operation::Insert),
            2 => Just(Operation::Get),
            2 => Just(Operation::Missing),
            1 => Just(Operation::Invalidate),
        ],
        0..KEY_COUNT,
        0..=SOURCE_LENGTH,
        0..=SOURCE_LENGTH,
        any::<u8>(),
    )
}

fn bounded_operation() -> impl Strategy<Value = (BoundedOperation, usize)> {
    (
        prop_oneof![
            4 => Just(BoundedOperation::Insert),
            4 => Just(BoundedOperation::Get),
            2 => Just(BoundedOperation::Invalidate),
            1 => Just(BoundedOperation::Clear),
        ],
        0..5_usize,
    )
}

fn model_missing(bytes: &[Option<u8>], start: usize, end: usize) -> Vec<std::ops::Range<usize>> {
    let mut missing = Vec::new();
    let mut cursor = start;
    while cursor < end {
        if bytes[cursor].is_some() {
            cursor += 1;
            continue;
        }
        let range_start = cursor;
        while cursor < end && bytes[cursor].is_none() {
            cursor += 1;
        }
        missing.push(range_start..cursor);
    }
    missing
}

fn model_range_count(model: &[[Option<u8>; SOURCE_LENGTH]; KEY_COUNT]) -> usize {
    model
        .iter()
        .map(|bytes| {
            bytes
                .iter()
                .enumerate()
                .filter(|(index, byte)| {
                    byte.is_some() && (*index == 0 || bytes[*index - 1].is_none())
                })
                .count()
        })
        .sum()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn randomized_operations_match_reference_coverage(
        operations in prop::collection::vec(operation(), 1..200),
    ) {
        let cache = RangeCache::new(CacheCapacity::Unbounded);
        let mut model = [[None; SOURCE_LENGTH]; KEY_COUNT];

        for (operation, key, first, second, value) in operations {
            let start = first.min(second);
            let end = first.max(second);
            match operation {
                Operation::Insert => {
                    let already_covered = model[key][start..end].iter().all(Option::is_some);
                    let payload = Bytes::from(
                        (start..end)
                            .map(|offset| {
                                value.wrapping_add(
                                    u8::try_from(offset).expect("model offset fits in u8"),
                                )
                            })
                            .collect::<Vec<_>>(),
                    );
                    let outcome = cache.insert(key, start..end, payload).expect("valid insert");
                    prop_assert_eq!(
                        outcome,
                        if already_covered {
                            InsertOutcome::AlreadyCovered
                        } else {
                            InsertOutcome::Inserted
                        }
                    );
                    if !already_covered {
                        for (offset, byte) in model[key][start..end].iter_mut().enumerate() {
                            *byte = Some(value.wrapping_add(
                                u8::try_from(start + offset).expect("model offset fits in u8"),
                            ));
                        }
                    }
                }
                Operation::Get => {
                    let expected = model[key][start..end]
                        .iter()
                        .copied()
                        .collect::<Option<Vec<_>>>()
                        .map(Bytes::from);
                    prop_assert_eq!(cache.get(&key, start..end).expect("valid range"), expected);
                }
                Operation::Missing => {
                    prop_assert_eq!(
                        cache.missing_ranges(&key, start..end).expect("valid range"),
                        model_missing(&model[key], start, end)
                    );
                }
                Operation::Invalidate => {
                    let _ = cache.invalidate(&key);
                    model[key].fill(None);
                }
            }

            let snapshot = cache.snapshot();
            let resident_bytes = model
                .iter()
                .flatten()
                .filter(|byte| byte.is_some())
                .count();
            prop_assert_eq!(snapshot.resident_bytes, resident_bytes);
            prop_assert_eq!(
                snapshot.keys,
                model.iter().filter(|bytes| bytes.iter().any(Option::is_some)).count()
            );
            prop_assert_eq!(snapshot.ranges, model_range_count(&model));
        }
    }

    #[test]
    fn bounded_cache_matches_reference_lru(
        operations in prop::collection::vec(bounded_operation(), 1..200),
    ) {
        let cache = RangeCache::new(CacheCapacity::Bounded(
            NonZeroUsize::new(8).expect("test capacity is non-zero"),
        ));
        let mut access_by_key = [None; 5];
        let mut next_access = 0_u64;
        let mut hits = 0_u64;
        let mut misses = 0_u64;
        let mut insertions = 0_u64;
        let mut evictions = 0_u64;

        for (operation, key) in operations {
            match operation {
                BoundedOperation::Insert => {
                    let outcome = cache
                        .insert(
                            key,
                            0..4,
                            Bytes::from(vec![u8::try_from(key).expect("key fits in u8"); 4]),
                        )
                        .expect("valid insert");
                    if access_by_key[key].is_some() {
                        prop_assert_eq!(outcome, InsertOutcome::AlreadyCovered);
                    } else {
                        prop_assert_eq!(outcome, InsertOutcome::Inserted);
                        access_by_key[key] = Some(next_access);
                        next_access += 1;
                        insertions += 1;

                        if access_by_key.iter().flatten().count() > 2 {
                            let (oldest_key, _) = access_by_key
                                .iter()
                                .enumerate()
                                .filter_map(|(key, access)| access.map(|access| (key, access)))
                                .min_by_key(|(_, access)| *access)
                                .expect("an over-capacity model has an oldest entry");
                            access_by_key[oldest_key] = None;
                            evictions += 1;
                        }
                    }
                }
                BoundedOperation::Get => {
                    let expected = access_by_key[key]
                        .is_some()
                        .then(|| {
                            Bytes::from(vec![u8::try_from(key).expect("key fits in u8"); 4])
                        });
                    prop_assert_eq!(cache.get(&key, 0..4).expect("valid range"), expected);
                    if access_by_key[key].is_some() {
                        access_by_key[key] = Some(next_access);
                        next_access += 1;
                        hits += 1;
                    } else {
                        misses += 1;
                    }
                }
                BoundedOperation::Invalidate => {
                    let expected = if access_by_key[key].take().is_some() {
                        Invalidation {
                            ranges: 1,
                            bytes: 4,
                        }
                    } else {
                        Invalidation::default()
                    };
                    prop_assert_eq!(cache.invalidate(&key), expected);
                }
                BoundedOperation::Clear => {
                    let ranges = access_by_key.iter().flatten().count();
                    prop_assert_eq!(
                        cache.clear(),
                        Invalidation {
                            ranges,
                            bytes: ranges * 4,
                        }
                    );
                    access_by_key.fill(None);
                }
            }

            let ranges = access_by_key.iter().flatten().count();
            let snapshot = cache.snapshot();
            prop_assert_eq!(snapshot.resident_bytes, ranges * 4);
            prop_assert_eq!(snapshot.keys, ranges);
            prop_assert_eq!(snapshot.ranges, ranges);
            prop_assert_eq!(snapshot.hits, hits);
            prop_assert_eq!(snapshot.partial_hits, 0);
            prop_assert_eq!(snapshot.misses, misses);
            prop_assert_eq!(snapshot.insertions, insertions);
            prop_assert_eq!(snapshot.admissions_rejected_too_large, 0);
            prop_assert_eq!(snapshot.evictions, evictions);
        }
    }
}
