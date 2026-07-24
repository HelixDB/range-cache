//! Core cache types.

use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    ops::{Bound, Range},
    sync::Arc,
};

use bytes::Bytes;
use parking_lot::Mutex;

use crate::RangeError;

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

struct CacheBlock {
    end: usize,
    bytes: Bytes,
    last_access: Option<u64>,
}

#[derive(Default)]
struct Statistics {
    hits: u64,
    partial_hits: u64,
    misses: u64,
    insertions: u64,
    admissions_rejected_too_large: u64,
    evictions: u64,
}

enum EvictionPolicy<K> {
    Unbounded,
    Bounded {
        lru: BTreeMap<u64, (K, usize)>,
        next_access: u64,
    },
}

struct State<K> {
    ranges: BTreeMap<K, BTreeMap<usize, CacheBlock>>,
    eviction: EvictionPolicy<K>,
    resident_bytes: usize,
    resident_ranges: usize,
    statistics: Statistics,
}

impl<K> State<K> {
    fn new(capacity: CacheCapacity) -> Self {
        Self {
            ranges: BTreeMap::new(),
            eviction: match capacity {
                CacheCapacity::Bounded(_) => EvictionPolicy::Bounded {
                    lru: BTreeMap::new(),
                    next_access: 0,
                },
                CacheCapacity::Unbounded => EvictionPolicy::Unbounded,
            },
            resident_bytes: 0,
            resident_ranges: 0,
            statistics: Statistics::default(),
        }
    }
}

impl<K: Ord + Clone> State<K> {
    fn take_access(&mut self) -> Option<u64> {
        let EvictionPolicy::Bounded { next_access, .. } = &mut self.eviction else {
            return None;
        };
        let access = *next_access;
        *next_access = next_access
            .checked_add(1)
            .expect("range cache LRU clock exhausted");
        Some(access)
    }

    fn touch(&mut self, key: &K, start: usize) {
        if matches!(self.eviction, EvictionPolicy::Unbounded) {
            return;
        }

        let Some(previous) = self
            .ranges
            .get(key)
            .and_then(|ranges| ranges.get(&start))
            .and_then(|block| block.last_access)
        else {
            panic!("bounded resident range must have an access value");
        };

        let EvictionPolicy::Bounded { lru, .. } = &mut self.eviction else {
            unreachable!("unbounded policy returned before touching recency");
        };
        let Some((resident_key, resident_start)) = lru.remove(&previous) else {
            panic!("resident range must have an LRU entry");
        };
        assert!(
            &resident_key == key && resident_start == start,
            "LRU entry must identify the resident range"
        );
        let next = self
            .take_access()
            .expect("bounded cache provides access values");
        self.ranges
            .get_mut(key)
            .and_then(|ranges| ranges.get_mut(&start))
            .expect("touched range remains resident")
            .last_access = Some(next);
        let EvictionPolicy::Bounded { lru, .. } = &mut self.eviction else {
            unreachable!("bounded policy remains bounded");
        };
        assert!(
            lru.insert(next, (resident_key, start)).is_none(),
            "LRU access value is unique"
        );
    }

    fn take_block(&mut self, key: &K, start: usize) -> Option<CacheBlock> {
        let (block, key_is_empty) = {
            let ranges = self.ranges.get_mut(key)?;
            let block = ranges.remove(&start)?;
            (block, ranges.is_empty())
        };

        if key_is_empty {
            self.ranges.remove(key);
        }
        self.resident_bytes = self
            .resident_bytes
            .checked_sub(block.bytes.len())
            .expect("resident byte accounting cannot underflow");
        self.resident_ranges = self
            .resident_ranges
            .checked_sub(1)
            .expect("resident range accounting cannot underflow");
        Some(block)
    }

    fn remove(&mut self, key: &K, start: usize) -> Option<CacheBlock> {
        let block = self.take_block(key, start)?;
        match (&mut self.eviction, block.last_access) {
            (EvictionPolicy::Unbounded, None) => {}
            (EvictionPolicy::Bounded { lru, .. }, Some(access)) => {
                let Some((resident_key, resident_start)) = lru.remove(&access) else {
                    panic!("removed range must have an LRU entry");
                };
                assert!(
                    &resident_key == key && resident_start == start,
                    "LRU entry must identify the removed range"
                );
            }
            (EvictionPolicy::Unbounded, Some(_)) | (EvictionPolicy::Bounded { .. }, None) => {
                panic!("range access metadata must match the eviction policy");
            }
        }
        Some(block)
    }

    fn evict_oldest(&mut self) -> CacheBlock {
        let EvictionPolicy::Bounded { lru, .. } = &mut self.eviction else {
            panic!("only bounded caches evict");
        };
        let Some((access, (key, start))) = lru.pop_first() else {
            panic!("resident bytes require an LRU entry");
        };
        let block = self
            .take_block(&key, start)
            .expect("LRU range remains resident");
        assert_eq!(
            block.last_access,
            Some(access),
            "evicted range access value matches its LRU entry"
        );
        block
    }

    fn covered(&self, key: &K, requested: &Range<usize>) -> Vec<(usize, Range<usize>)> {
        let Some(ranges) = self.ranges.get(key) else {
            return Vec::new();
        };

        let mut covered = Vec::new();
        match ranges.range(..=requested.start).next_back() {
            Some((&start, block)) if block.end > requested.start => {
                covered.push((start, requested.start..block.end.min(requested.end)));
            }
            Some(_) | None => {}
        }

        covered.extend(
            ranges
                .range((
                    Bound::Excluded(requested.start),
                    Bound::Excluded(requested.end),
                ))
                .map(|(&start, block)| {
                    (
                        start,
                        start.max(requested.start)..block.end.min(requested.end),
                    )
                }),
        );
        covered
    }
}

/// A cloneable, thread-safe sparse cache of byte ranges keyed by `K`.
pub struct RangeCache<K> {
    inner: Arc<Mutex<State<K>>>,
    capacity: CacheCapacity,
}

impl<K> Clone for RangeCache<K> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            capacity: self.capacity,
        }
    }
}

impl<K> RangeCache<K> {
    /// Creates an empty cache with an explicit capacity policy.
    #[must_use]
    pub fn new(capacity: CacheCapacity) -> Self {
        Self {
            inner: Arc::new(Mutex::new(State::new(capacity))),
            capacity,
        }
    }

    /// Returns the configured capacity policy.
    #[must_use]
    pub const fn capacity(&self) -> CacheCapacity {
        self.capacity
    }
}

impl<K: Ord + Clone> RangeCache<K> {
    /// Returns the requested bytes when the entire range is cached.
    ///
    /// A hit contained in one resident range returns a zero-copy [`Bytes`]
    /// slice. Empty ranges always succeed.
    ///
    /// # Errors
    ///
    /// Returns [`RangeError::ReversedRange`] when `range.end < range.start`.
    pub fn get(&self, key: &K, range: Range<usize>) -> Result<Option<Bytes>, RangeError> {
        validate_range(&range)?;
        let mut state = self.inner.lock();

        if range.is_empty() {
            state.statistics.hits += 1;
            return Ok(Some(Bytes::new()));
        }

        let hit = state.ranges.get(key).and_then(|ranges| {
            ranges
                .range(..=range.start)
                .next_back()
                .filter(|(_, block)| range.end <= block.end)
                .map(|(&start, block)| {
                    let offset = range.start - start;
                    (start, block.bytes.slice(offset..offset + range.len()))
                })
        });

        if let Some((start, bytes)) = hit {
            state.statistics.hits += 1;
            state.touch(key, start);
            return Ok(Some(bytes));
        }

        let covered = state.covered(key, &range);
        if covered.is_empty() {
            state.statistics.misses += 1;
        } else {
            state.statistics.partial_hits += 1;
            for (start, _) in covered {
                state.touch(key, start);
            }
        }
        Ok(None)
    }

    /// Returns the gaps within `range` that are not resident for `key`.
    ///
    /// # Errors
    ///
    /// Returns [`RangeError::ReversedRange`] when `range.end < range.start`.
    pub fn missing_ranges(
        &self,
        key: &K,
        range: Range<usize>,
    ) -> Result<Vec<Range<usize>>, RangeError> {
        validate_range(&range)?;
        if range.is_empty() {
            return Ok(Vec::new());
        }

        let state = self.inner.lock();
        let covered = state.covered(key, &range);
        Ok(missing_ranges(&range, &covered))
    }

    /// Inserts `bytes` for `range`, merging adjacent and overlapping ranges.
    ///
    /// An insert wholly contained by one existing range is ignored. Otherwise,
    /// inserted bytes replace overlapping bytes while cached prefix and suffix
    /// bytes remain intact.
    ///
    /// # Errors
    ///
    /// Returns [`RangeError::ReversedRange`] for a reversed range or
    /// [`RangeError::PayloadLengthMismatch`] when the payload length differs
    /// from the range length.
    ///
    /// # Panics
    ///
    /// Panics only if an internal range-map, LRU, or byte-accounting invariant
    /// has been violated.
    pub fn insert(
        &self,
        key: K,
        range: Range<usize>,
        bytes: Bytes,
    ) -> Result<InsertOutcome, RangeError> {
        validate_range(&range)?;
        let expected = range.len();
        if bytes.len() != expected {
            return Err(RangeError::PayloadLengthMismatch {
                range,
                expected,
                actual: bytes.len(),
            });
        }
        if range.is_empty() {
            return Ok(InsertOutcome::AlreadyCovered);
        }

        let mut state = self.inner.lock();
        let containing = state.ranges.get(&key).and_then(|ranges| {
            ranges
                .range(..=range.start)
                .next_back()
                .filter(|(_, block)| range.end <= block.end)
        });
        if containing.is_some() {
            return Ok(InsertOutcome::AlreadyCovered);
        }

        let mut merged_start = range.start;
        let mut merged_end = range.end;
        let mut affected = Vec::new();
        if let Some(ranges) = state.ranges.get(&key) {
            for (&start, block) in ranges {
                if block.end < merged_start {
                    continue;
                }
                if start > merged_end {
                    break;
                }
                merged_start = merged_start.min(start);
                merged_end = merged_end.max(block.end);
                affected.push((start, block.end, block.bytes.clone()));
            }
        }

        let merged_length = merged_end - merged_start;
        match self.capacity {
            CacheCapacity::Bounded(capacity) if merged_length > capacity.get() => {
                state.statistics.admissions_rejected_too_large += 1;
                return Ok(InsertOutcome::TooLarge);
            }
            CacheCapacity::Bounded(_) | CacheCapacity::Unbounded => {}
        }

        let merged_bytes =
            if affected.is_empty() || (merged_start == range.start && merged_end == range.end) {
                bytes
            } else {
                let mut merged = vec![0; merged_length];
                for (start, end, cached) in &affected {
                    let offset = start - merged_start;
                    merged[offset..offset + (end - start)].copy_from_slice(cached);
                }
                let offset = range.start - merged_start;
                merged[offset..offset + expected].copy_from_slice(&bytes);
                Bytes::from(merged)
            };

        for (start, _, _) in affected {
            state
                .remove(&key, start)
                .expect("affected range remains resident");
        }

        let access = state.take_access();
        if let Some(access) = access {
            let EvictionPolicy::Bounded { lru, .. } = &mut state.eviction else {
                unreachable!("access values belong to bounded caches");
            };
            assert!(
                lru.insert(access, (key.clone(), merged_start)).is_none(),
                "new range has a unique LRU entry"
            );
        }
        let previous = state.ranges.entry(key).or_default().insert(
            merged_start,
            CacheBlock {
                end: merged_end,
                bytes: merged_bytes,
                last_access: access,
            },
        );
        assert!(previous.is_none(), "merged range start must be vacant");
        state.resident_bytes += merged_length;
        state.resident_ranges += 1;
        state.statistics.insertions += 1;

        if let CacheCapacity::Bounded(capacity) = self.capacity {
            while state.resident_bytes > capacity.get() {
                let _ = state.evict_oldest();
                state.statistics.evictions += 1;
            }
        }

        Ok(InsertOutcome::Inserted)
    }

    /// Removes all ranges associated with `key`.
    ///
    /// # Panics
    ///
    /// Panics only if an internal range-map, LRU, or byte-accounting invariant
    /// has been violated.
    #[must_use]
    pub fn invalidate(&self, key: &K) -> Invalidation {
        let mut state = self.inner.lock();
        let starts = state
            .ranges
            .get(key)
            .map(|ranges| ranges.keys().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        let mut invalidation = Invalidation::default();
        for start in starts {
            let block = state
                .remove(key, start)
                .expect("invalidated range remains resident");
            invalidation.ranges += 1;
            invalidation.bytes += block.bytes.len();
        }
        invalidation
    }

    /// Removes every resident range while retaining accumulated statistics.
    #[must_use]
    pub fn clear(&self) -> Invalidation {
        let mut state = self.inner.lock();
        let invalidation = Invalidation {
            ranges: state.resident_ranges,
            bytes: state.resident_bytes,
        };
        state.ranges.clear();
        if let EvictionPolicy::Bounded { lru, .. } = &mut state.eviction {
            lru.clear();
        }
        state.resident_bytes = 0;
        state.resident_ranges = 0;
        invalidation
    }

    /// Returns a consistent snapshot of cache state and lifetime statistics.
    #[must_use]
    pub fn snapshot(&self) -> CacheSnapshot {
        let state = self.inner.lock();
        CacheSnapshot {
            capacity: self.capacity,
            resident_bytes: state.resident_bytes,
            keys: state.ranges.len(),
            ranges: state.resident_ranges,
            hits: state.statistics.hits,
            partial_hits: state.statistics.partial_hits,
            misses: state.statistics.misses,
            insertions: state.statistics.insertions,
            admissions_rejected_too_large: state.statistics.admissions_rejected_too_large,
            evictions: state.statistics.evictions,
        }
    }

    #[cfg(feature = "async")]
    pub(crate) fn read_plan(&self, key: &K, range: Range<usize>) -> Result<ReadPlan, RangeError> {
        validate_range(&range)?;
        let mut state = self.inner.lock();
        if range.is_empty() {
            state.statistics.hits += 1;
            return Ok(ReadPlan::Complete(Bytes::new()));
        }

        let hit = state.ranges.get(key).and_then(|ranges| {
            ranges
                .range(..=range.start)
                .next_back()
                .filter(|(_, block)| range.end <= block.end)
                .map(|(&start, block)| {
                    let offset = range.start - start;
                    (start, block.bytes.slice(offset..offset + range.len()))
                })
        });
        if let Some((start, bytes)) = hit {
            state.statistics.hits += 1;
            state.touch(key, start);
            return Ok(ReadPlan::Complete(bytes));
        }

        let covered = state.covered(key, &range);
        let missing = missing_ranges(&range, &covered);
        let cached = covered
            .iter()
            .map(|(start, covered_range)| {
                let block = state
                    .ranges
                    .get(key)
                    .and_then(|ranges| ranges.get(start))
                    .expect("covered range remains resident");
                let offset = covered_range.start - start;
                (
                    covered_range.clone(),
                    block.bytes.slice(offset..offset + covered_range.len()),
                )
            })
            .collect();

        if covered.is_empty() {
            state.statistics.misses += 1;
        } else {
            state.statistics.partial_hits += 1;
            for (start, _) in covered {
                state.touch(key, start);
            }
        }
        Ok(ReadPlan::Fetch { cached, missing })
    }
}

fn validate_range(range: &Range<usize>) -> Result<(), RangeError> {
    if range.start > range.end {
        return Err(RangeError::ReversedRange {
            start: range.start,
            end: range.end,
        });
    }
    Ok(())
}

fn missing_ranges(
    requested: &Range<usize>,
    covered: &[(usize, Range<usize>)],
) -> Vec<Range<usize>> {
    let mut missing = Vec::new();
    let mut cursor = requested.start;
    for (_, range) in covered {
        if cursor < range.start {
            missing.push(cursor..range.start);
        }
        cursor = cursor.max(range.end);
    }
    if cursor < requested.end {
        missing.push(cursor..requested.end);
    }
    missing
}

#[cfg(feature = "async")]
pub(crate) enum ReadPlan {
    Complete(Bytes),
    Fetch {
        cached: Vec<(Range<usize>, Bytes)>,
        missing: Vec<Range<usize>>,
    },
}
