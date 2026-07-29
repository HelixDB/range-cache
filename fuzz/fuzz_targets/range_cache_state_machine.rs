#![no_main]

use std::{collections::BTreeMap, num::NonZeroUsize, ops::Range};

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use range_cache::{
    CacheCapacity, CacheSnapshot, InsertOutcome, Invalidation, RangeCache, RangeError,
};

const DOMAIN: usize = 32;
const KEY_COUNT: usize = 4;

#[derive(Clone, Copy)]
struct ModelRange {
    end: usize,
    last_access: u64,
}

struct Model {
    base: usize,
    capacity: CacheCapacity,
    bytes: [[Option<u8>; DOMAIN]; KEY_COUNT],
    ranges: [BTreeMap<usize, ModelRange>; KEY_COUNT],
    next_access: u64,
    hits: u64,
    partial_hits: u64,
    misses: u64,
    insertions: u64,
    admissions_rejected_too_large: u64,
    evictions: u64,
}

impl Model {
    fn new(base: usize, capacity: CacheCapacity) -> Self {
        Self {
            base,
            capacity,
            bytes: [[None; DOMAIN]; KEY_COUNT],
            ranges: std::array::from_fn(|_| BTreeMap::new()),
            next_access: 0,
            hits: 0,
            partial_hits: 0,
            misses: 0,
            insertions: 0,
            admissions_rejected_too_large: 0,
            evictions: 0,
        }
    }

    fn absolute(&self, offset: usize) -> usize {
        self.base + offset
    }

    fn range_error(&self, start: usize, end: usize) -> RangeError {
        RangeError::ReversedRange {
            start: self.absolute(start),
            end: self.absolute(end),
        }
    }

    fn insert(
        &mut self,
        key: usize,
        start: usize,
        end: usize,
        payload: &[u8],
    ) -> Result<InsertOutcome, RangeError> {
        if start > end {
            return Err(self.range_error(start, end));
        }
        let expected = end - start;
        if payload.len() != expected {
            return Err(RangeError::PayloadLengthMismatch {
                range: self.absolute(start)..self.absolute(end),
                expected,
                actual: payload.len(),
            });
        }
        if start == end {
            return Ok(InsertOutcome::AlreadyCovered);
        }

        let containing = self.ranges[key]
            .range(..=start)
            .next_back()
            .is_some_and(|(_, block)| end <= block.end);
        if containing {
            return Ok(InsertOutcome::AlreadyCovered);
        }

        let mut merged_start = start;
        let mut merged_end = end;
        let mut affected = Vec::new();
        for (&block_start, block) in &self.ranges[key] {
            if block.end < merged_start {
                continue;
            }
            if block_start > merged_end {
                break;
            }
            merged_start = merged_start.min(block_start);
            merged_end = merged_end.max(block.end);
            affected.push(block_start);
        }

        let merged_len = merged_end - merged_start;
        if matches!(self.capacity, CacheCapacity::Bounded(capacity) if merged_len > capacity.get())
        {
            self.admissions_rejected_too_large += 1;
            return Ok(InsertOutcome::TooLarge);
        }

        self.bytes[key][start..end]
            .iter_mut()
            .zip(payload)
            .for_each(|(slot, &byte)| *slot = Some(byte));
        for block_start in affected {
            self.ranges[key].remove(&block_start);
        }
        let access = self.next_access;
        self.next_access += 1;
        self.ranges[key].insert(
            merged_start,
            ModelRange {
                end: merged_end,
                last_access: access,
            },
        );
        self.insertions += 1;

        if let CacheCapacity::Bounded(capacity) = self.capacity {
            while self.resident_bytes() > capacity.get() {
                let (oldest_key, oldest_start, oldest_end) = self
                    .ranges
                    .iter()
                    .enumerate()
                    .flat_map(|(key, ranges)| {
                        ranges
                            .iter()
                            .map(move |(&start, block)| (key, start, block.end, block.last_access))
                    })
                    .min_by_key(|(_, _, _, access)| *access)
                    .map(|(key, start, end, _)| (key, start, end))
                    .expect("over-capacity model has a resident range");
                self.ranges[oldest_key].remove(&oldest_start);
                self.bytes[oldest_key][oldest_start..oldest_end].fill(None);
                self.evictions += 1;
            }
        }
        Ok(InsertOutcome::Inserted)
    }

    fn get(&mut self, key: usize, start: usize, end: usize) -> Result<Option<Vec<u8>>, RangeError> {
        if start > end {
            return Err(self.range_error(start, end));
        }
        if start == end {
            self.hits += 1;
            return Ok(Some(Vec::new()));
        }

        let hit = self.ranges[key]
            .range(..=start)
            .next_back()
            .filter(|(_, block)| end <= block.end)
            .map(|(&block_start, _)| block_start);
        if let Some(block_start) = hit {
            self.touch(key, block_start);
            self.hits += 1;
            return Ok(Some(
                self.bytes[key][start..end]
                    .iter()
                    .map(|byte| byte.expect("covered model byte exists"))
                    .collect(),
            ));
        }

        let covered = self.ranges[key]
            .iter()
            .filter(|(block_start, block)| **block_start < end && block.end > start)
            .map(|(&block_start, _)| block_start)
            .collect::<Vec<_>>();
        if covered.is_empty() {
            self.misses += 1;
        } else {
            for block_start in covered {
                self.touch(key, block_start);
            }
            self.partial_hits += 1;
        }
        Ok(None)
    }

    fn missing(
        &self,
        key: usize,
        start: usize,
        end: usize,
    ) -> Result<Vec<Range<usize>>, RangeError> {
        if start > end {
            return Err(self.range_error(start, end));
        }
        let mut missing = Vec::new();
        let mut cursor = start;
        while cursor < end {
            if self.bytes[key][cursor].is_some() {
                cursor += 1;
                continue;
            }
            let gap_start = cursor;
            while cursor < end && self.bytes[key][cursor].is_none() {
                cursor += 1;
            }
            missing.push(self.absolute(gap_start)..self.absolute(cursor));
        }
        Ok(missing)
    }

    fn invalidate(&mut self, key: usize) -> Invalidation {
        let invalidation = Invalidation {
            ranges: self.ranges[key].len(),
            bytes: self.bytes[key].iter().flatten().count(),
        };
        self.ranges[key].clear();
        self.bytes[key].fill(None);
        invalidation
    }

    fn clear(&mut self) -> Invalidation {
        let invalidation = Invalidation {
            ranges: self.ranges.iter().map(BTreeMap::len).sum(),
            bytes: self.resident_bytes(),
        };
        self.ranges.iter_mut().for_each(BTreeMap::clear);
        self.bytes.iter_mut().for_each(|bytes| bytes.fill(None));
        invalidation
    }

    fn touch(&mut self, key: usize, start: usize) {
        let block = self.ranges[key]
            .get_mut(&start)
            .expect("touched model range exists");
        block.last_access = self.next_access;
        self.next_access += 1;
    }

    fn resident_bytes(&self) -> usize {
        self.bytes.iter().flatten().flatten().count()
    }

    fn snapshot(&self) -> CacheSnapshot {
        CacheSnapshot {
            capacity: self.capacity,
            resident_bytes: self.resident_bytes(),
            keys: self
                .ranges
                .iter()
                .filter(|ranges| !ranges.is_empty())
                .count(),
            ranges: self.ranges.iter().map(BTreeMap::len).sum(),
            hits: self.hits,
            partial_hits: self.partial_hits,
            misses: self.misses,
            insertions: self.insertions,
            admissions_rejected_too_large: self.admissions_rejected_too_large,
            evictions: self.evictions,
        }
    }
}

fuzz_target!(|data: &[u8]| {
    let Some((&configuration, operations)) = data.split_first() else {
        return;
    };
    let capacity = if configuration & 1 == 0 {
        CacheCapacity::Unbounded
    } else {
        CacheCapacity::Bounded(
            NonZeroUsize::new(usize::from(configuration >> 1) % DOMAIN + 1)
                .expect("derived capacity is non-zero"),
        )
    };
    let base = if configuration & 0x80 == 0 {
        0
    } else {
        usize::MAX - DOMAIN
    };
    let cache = RangeCache::new(capacity);
    let mut model = Model::new(base, capacity);

    for operation in operations.chunks_exact(6).take(256) {
        let key = usize::from(operation[1]) % KEY_COUNT;
        let start = usize::from(operation[2]) % (DOMAIN + 1);
        let end = usize::from(operation[3]) % (DOMAIN + 1);
        let absolute = model.absolute(start)..model.absolute(end);

        match operation[0] % 5 {
            0 => {
                let range_len = end.saturating_sub(start);
                let payload_len = match operation[4] % 3 {
                    0 => range_len,
                    1 => range_len.saturating_sub(1),
                    _ => (range_len + 1).min(DOMAIN + 1),
                };
                let payload = (0..payload_len)
                    .map(|offset| operation[5].wrapping_add(offset as u8))
                    .collect::<Vec<_>>();
                assert_eq!(
                    cache.insert(key as u8, absolute, Bytes::from(payload.clone())),
                    model.insert(key, start, end, &payload)
                );
            }
            1 => {
                let expected = model
                    .get(key, start, end)
                    .map(|bytes| bytes.map(Bytes::from));
                assert_eq!(cache.get(&(key as u8), absolute), expected);
            }
            2 => {
                assert_eq!(
                    cache.missing_ranges(&(key as u8), absolute),
                    model.missing(key, start, end)
                );
            }
            3 => {
                assert_eq!(cache.invalidate(&(key as u8)), model.invalidate(key));
            }
            4 => {
                assert_eq!(cache.clear(), model.clear());
            }
            _ => unreachable!("opcode is reduced modulo five"),
        }

        assert_eq!(cache.snapshot(), model.snapshot());
        for checked_key in 0..KEY_COUNT {
            assert_eq!(
                cache
                    .missing_ranges(
                        &(checked_key as u8),
                        model.absolute(0)..model.absolute(DOMAIN),
                    )
                    .expect("model verification range is valid"),
                model
                    .missing(checked_key, 0, DOMAIN)
                    .expect("model verification range is valid")
            );
        }
    }
});
