use std::{collections::BTreeMap, num::NonZeroUsize, ops::Range};

use bytes::Bytes;
use proptest::prelude::*;
use range_cache::{CacheCapacity, InsertOutcome, Invalidation, RangeCache};

const KEY_COUNT: usize = 3;
const SOURCE_LENGTH: usize = 32;
const BOUNDED_KEY_COUNT: usize = 3;
const SLOT_COUNT: usize = 4;

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
    Missing,
    Invalidate,
    Clear,
}

#[derive(Clone, Copy, Debug)]
enum SparseOperation {
    Insert,
    Get,
    Missing,
    Invalidate,
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

fn bounded_operation() -> impl Strategy<Value = (BoundedOperation, usize, usize, usize)> {
    (
        prop_oneof![
            4 => Just(BoundedOperation::Insert),
            4 => Just(BoundedOperation::Get),
            2 => Just(BoundedOperation::Missing),
            2 => Just(BoundedOperation::Invalidate),
            1 => Just(BoundedOperation::Clear),
        ],
        0..BOUNDED_KEY_COUNT,
        0..SLOT_COUNT,
        0..SLOT_COUNT,
    )
}

fn sparse_operation() -> impl Strategy<Value = (SparseOperation, usize, usize, u8)> {
    (
        prop_oneof![
            4 => Just(SparseOperation::Insert),
            2 => Just(SparseOperation::Get),
            2 => Just(SparseOperation::Missing),
            1 => Just(SparseOperation::Invalidate),
        ],
        0..1_000_000_usize,
        1..65_usize,
        any::<u8>(),
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

fn sparse_missing(model: &BTreeMap<usize, u8>, range: Range<usize>) -> Vec<Range<usize>> {
    let mut missing = Vec::new();
    let mut cursor = range.start;
    while cursor < range.end {
        if model.contains_key(&cursor) {
            cursor += 1;
            continue;
        }
        let start = cursor;
        while cursor < range.end && !model.contains_key(&cursor) {
            cursor += 1;
        }
        missing.push(start..cursor);
    }
    missing
}

fn sparse_range_count(model: &BTreeMap<usize, u8>) -> usize {
    model
        .keys()
        .scan(None, |previous, &offset| {
            let starts_range = previous.is_none_or(|previous| previous + 1 != offset);
            *previous = Some(offset);
            Some(starts_range)
        })
        .filter(|starts_range| *starts_range)
        .count()
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
        let mut access_by_range = [[None; SLOT_COUNT]; BOUNDED_KEY_COUNT];
        let mut next_access = 0_u64;
        let mut hits = 0_u64;
        let mut partial_hits = 0_u64;
        let mut misses = 0_u64;
        let mut insertions = 0_u64;
        let mut evictions = 0_u64;

        for (operation, key, first, second) in operations {
            let first_slot = first.min(second);
            let last_slot = first.max(second);
            let range = first_slot * 4..last_slot * 4 + 2;
            match operation {
                BoundedOperation::Insert => {
                    let slot = first;
                    let block_start = slot * 4;
                    let payload = Bytes::from(vec![
                        u8::try_from(key).expect("key fits in u8"),
                        u8::try_from(slot).expect("slot fits in u8"),
                    ]);
                    let outcome = cache
                        .insert(key, block_start..block_start + 2, payload)
                        .expect("valid insert");
                    if access_by_range[key][slot].is_some() {
                        prop_assert_eq!(outcome, InsertOutcome::AlreadyCovered);
                    } else {
                        prop_assert_eq!(outcome, InsertOutcome::Inserted);
                        access_by_range[key][slot] = Some(next_access);
                        next_access += 1;
                        insertions += 1;

                        if access_by_range.iter().flatten().flatten().count() > 4 {
                            let (oldest_key, oldest_slot, _) = access_by_range
                                .iter()
                                .enumerate()
                                .flat_map(|(key, ranges)| {
                                    ranges.iter().enumerate().filter_map(
                                        move |(slot, access)| {
                                            access.map(|access| (key, slot, access))
                                        },
                                    )
                                })
                                .min_by_key(|(_, _, access)| *access)
                                .expect("an over-capacity model has an oldest entry");
                            access_by_range[oldest_key][oldest_slot] = None;
                            evictions += 1;
                        }
                    }
                }
                BoundedOperation::Get => {
                    let resident_slots = (first_slot..=last_slot)
                        .filter(|&slot| access_by_range[key][slot].is_some())
                        .collect::<Vec<_>>();
                    let expected = (first_slot == last_slot
                        && access_by_range[key][first_slot].is_some())
                    .then(|| {
                        Bytes::from(vec![
                            u8::try_from(key).expect("key fits in u8"),
                            u8::try_from(first_slot).expect("slot fits in u8"),
                        ])
                    });
                    let is_hit = expected.is_some();
                    prop_assert_eq!(cache.get(&key, range).expect("valid range"), expected);
                    if is_hit {
                        access_by_range[key][first_slot] = Some(next_access);
                        next_access += 1;
                        hits += 1;
                    } else if resident_slots.is_empty() {
                        misses += 1;
                    } else {
                        for slot in resident_slots {
                            access_by_range[key][slot] = Some(next_access);
                            next_access += 1;
                        }
                        partial_hits += 1;
                    }
                }
                BoundedOperation::Missing => {
                    let mut expected = Vec::new();
                    let mut cursor = range.start;
                    for (slot, access) in access_by_range[key]
                        .iter()
                        .enumerate()
                        .take(last_slot + 1)
                        .skip(first_slot)
                    {
                        let start = slot * 4;
                        if access.is_none() {
                            continue;
                        }
                        if cursor < start {
                            expected.push(cursor..start);
                        }
                        cursor = start + 2;
                    }
                    if cursor < range.end {
                        expected.push(cursor..range.end);
                    }
                    prop_assert_eq!(
                        cache.missing_ranges(&key, range).expect("valid range"),
                        expected
                    );
                }
                BoundedOperation::Invalidate => {
                    let ranges = access_by_range[key].iter().flatten().count();
                    let expected = Invalidation {
                        ranges,
                        bytes: ranges * 2,
                    };
                    prop_assert_eq!(cache.invalidate(&key), expected);
                    access_by_range[key].fill(None);
                }
                BoundedOperation::Clear => {
                    let ranges = access_by_range.iter().flatten().flatten().count();
                    prop_assert_eq!(
                        cache.clear(),
                        Invalidation {
                            ranges,
                            bytes: ranges * 2,
                        }
                    );
                    access_by_range.fill([None; SLOT_COUNT]);
                }
            }

            let ranges = access_by_range.iter().flatten().flatten().count();
            let snapshot = cache.snapshot();
            prop_assert_eq!(snapshot.resident_bytes, ranges * 2);
            prop_assert_eq!(
                snapshot.keys,
                access_by_range
                    .iter()
                    .filter(|ranges| ranges.iter().any(Option::is_some))
                    .count()
            );
            prop_assert_eq!(snapshot.ranges, ranges);
            prop_assert_eq!(snapshot.hits, hits);
            prop_assert_eq!(snapshot.partial_hits, partial_hits);
            prop_assert_eq!(snapshot.misses, misses);
            prop_assert_eq!(snapshot.insertions, insertions);
            prop_assert_eq!(snapshot.admissions_rejected_too_large, 0);
            prop_assert_eq!(snapshot.evictions, evictions);
        }
    }

    #[test]
    fn sparse_ranges_match_reference_coverage(
        operations in prop::collection::vec(sparse_operation(), 1..100),
    ) {
        let cache = RangeCache::new(CacheCapacity::Unbounded);
        let mut model = BTreeMap::new();

        for (operation, start, length, value) in operations {
            let end = start + length;
            match operation {
                SparseOperation::Insert => {
                    let already_covered = (start..end).all(|offset| model.contains_key(&offset));
                    let outcome = cache
                        .insert((), start..end, Bytes::from(vec![value; length]))
                        .expect("valid insert");
                    prop_assert_eq!(
                        outcome,
                        if already_covered {
                            InsertOutcome::AlreadyCovered
                        } else {
                            InsertOutcome::Inserted
                        }
                    );
                    if !already_covered {
                        model.extend((start..end).map(|offset| (offset, value)));
                    }
                }
                SparseOperation::Get => {
                    let expected = (start..end)
                        .map(|offset| model.get(&offset).copied())
                        .collect::<Option<Vec<_>>>()
                        .map(Bytes::from);
                    prop_assert_eq!(cache.get(&(), start..end).expect("valid range"), expected);
                }
                SparseOperation::Missing => {
                    prop_assert_eq!(
                        cache.missing_ranges(&(), start..end).expect("valid range"),
                        sparse_missing(&model, start..end)
                    );
                }
                SparseOperation::Invalidate => {
                    let ranges = sparse_range_count(&model);
                    prop_assert_eq!(
                        cache.invalidate(&()),
                        Invalidation {
                            ranges,
                            bytes: model.len(),
                        }
                    );
                    model.clear();
                }
            }

            let snapshot = cache.snapshot();
            prop_assert_eq!(snapshot.resident_bytes, model.len());
            prop_assert_eq!(snapshot.keys, usize::from(!model.is_empty()));
            prop_assert_eq!(snapshot.ranges, sparse_range_count(&model));
        }
    }
}
