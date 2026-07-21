use bytes::Bytes;
use proptest::prelude::*;
use range_cache::{CacheCapacity, InsertOutcome, RangeCache};

const KEY_COUNT: usize = 3;
const SOURCE_LENGTH: usize = 32;

#[derive(Clone, Copy, Debug)]
enum Operation {
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
}
