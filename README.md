# range-cache

`range-cache` is a thread-safe sparse byte-range cache for immutable sources.
It stores `bytes::Bytes` under ordered keys, merges adjacent or overlapping
coverage, and can enforce a payload-byte ceiling with range-level LRU eviction.

The core is synchronous and runtime-independent. The optional `async` feature
adds an object-safe source trait and a read-through adapter that fetches only
missing gaps.

## Core cache

```rust
use std::num::NonZeroUsize;

use bytes::Bytes;
use range_cache::{CacheCapacity, InsertOutcome, RangeCache};

let cache = RangeCache::new(CacheCapacity::Bounded(
    NonZeroUsize::new(1024).expect("capacity is non-zero"),
));

assert_eq!(
    cache.insert("object", 4..8, Bytes::from_static(b"data"))?,
    InsertOutcome::Inserted,
);
assert_eq!(
    cache.get(&"object", 5..7)?,
    Some(Bytes::from_static(b"at")),
);
assert_eq!(cache.missing_ranges(&"object", 2..10)?, vec![2..4, 8..10]);
# Ok::<(), range_cache::RangeError>(())
```

Capacity must always be explicit. `CacheCapacity::Bounded` uses a non-zero
payload-byte ceiling; `CacheCapacity::Unbounded` is an intentional opt-in.
`RangeCache` therefore has no `Default` implementation.

### Range semantics

- Empty reads return empty bytes and matching empty inserts are no-ops.
- Reversed ranges and payload-length mismatches return `RangeError`.
- An insert wholly contained by one cached block is ignored.
- An extending or bridging insert replaces its overlap while preserving cached
  bytes outside the inserted range.
- Adjacent and overlapping blocks merge. Disjoint blocks remain separate.
- A merged block larger than a bounded cache is rejected without changing
  existing state.
- Full single-block hits return zero-copy `Bytes` slices.

Mutable sources must invalidate a key before reads from a new version. The
crate intentionally does not infer versions or promise cross-version
coherence.

## Async read-through

Enable the optional layer with:

```toml
[dependencies]
range-cache = { version = "0.1", features = ["async"] }
```

Implement `RangeReader<K>` for an immutable source, then wrap it in
`CachedReader`. The wrapper validates exact-length responses, fetches missing
gaps under a global concurrency limit, and coalesces identical key-plus-gap
requests. Source errors and short reads are never cached. Overlapping requests
that are not identical remain independent.

The async layer uses Tokio synchronization primitives but does not spawn tasks
or require a particular executor for `CachedReader::read`.

## Observability

`RangeCache::snapshot` returns a consistent view of capacity, resident bytes,
key and range counts, hits, partial hits, misses, insertions, oversized
admission rejections, and evictions. Statistics are retained across `clear`.

## Compatibility

- Rust 1.85 or newer
- Rust edition 2024
- Default features: none
- License: Apache-2.0

See [CHANGELOG.md](CHANGELOG.md) for release notes and
[CONTRIBUTING.md](CONTRIBUTING.md) for development commands.

