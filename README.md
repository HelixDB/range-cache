# range-cache

`range-cache` is a thread-safe sparse byte-range cache for immutable sources.
It stores `bytes::Bytes` under ordered keys, merges adjacent or overlapping
coverage, and can enforce a payload-byte ceiling with range-level LRU eviction.

Use it when a query engine repeatedly reads small, overlapping regions of the
same immutable object—for example, index blocks stored in S3. Instead of
downloading or caching the whole object, `range-cache` retains only fetched
ranges, serves later overlaps from memory, and coalesces identical concurrent
misses into one source read.

The core is synchronous and runtime-independent. The optional `async` feature
adds an object-safe source trait and a read-through adapter that fetches only
missing gaps.

## Core cache

```rust
use std::num::NonZeroUsize;

use bytes::Bytes;
use range_cache::{CacheCapacity, InsertOutcome, RangeCache, RangeError};

fn main() -> Result<(), RangeError> {
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
    assert_eq!(
        cache.missing_ranges(&"object", 2..10)?,
        vec![2..4, 8..10],
    );
    Ok(())
}
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
async-trait = "0.1"
bytes = "1"
range-cache = { version = "0.1", features = ["async"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

Implement `RangeReader<K>` for an immutable source, then wrap it in
`CachedReader`. The wrapper validates exact-length responses, fetches missing
gaps under a global concurrency limit, and coalesces identical key-plus-gap
requests. Source errors and short reads are never cached. Overlapping requests
that are not identical remain independent.

The async layer uses Tokio synchronization primitives but does not spawn tasks
or require a particular executor for `CachedReader::read`.

```rust
#[cfg(feature = "async")]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::{
        convert::Infallible,
        num::NonZeroUsize,
        ops::Range,
        sync::Arc,
    };

    use bytes::Bytes;
    use range_cache::{CacheCapacity, CachedReader, RangeCache, RangeReader, ReaderConfig};

    struct MemorySource(Bytes);

    #[async_trait::async_trait]
    impl RangeReader<String> for MemorySource {
        type Error = Infallible;

        async fn read_range(
            &self,
            _key: &String,
            range: Range<usize>,
        ) -> Result<Bytes, Self::Error> {
            Ok(self.0.slice(range))
        }
    }

    let cache = RangeCache::new(CacheCapacity::Bounded(
        NonZeroUsize::new(1024).expect("capacity is non-zero"),
    ));
    let source = Arc::new(MemorySource(Bytes::from_static(b"abcdefghijklmnop")));
    let reader = CachedReader::new(
        source,
        cache,
        ReaderConfig::new(NonZeroUsize::new(4).expect("concurrency is non-zero")),
    );
    let key = String::from("s3://bucket/index");

    assert_eq!(
        reader.read(&key, 4..12).await?,
        Bytes::from_static(b"efghijkl"),
    );
    Ok(())
}

#[cfg(not(feature = "async"))]
fn main() {}
```

## Microbenchmarks

The table below reports the median of three Criterion median point estimates.
Setup and fixture destruction are excluded from measured times. Full-hit
latency is reported instead of apparent byte throughput because the returned
`Bytes` value is a zero-copy slice.

| Operation | Workload | Median estimate |
| --- | --- | ---: |
| Full hit | 32 KiB requested from a 64 KiB cached range | 19.89 ns |
| Gap calculation | 64 resident ranges | 498.41 ns |
| Overlapping insertion | Merge across 64 resident ranges | 3.16 µs |
| Eviction | Insert with 64 resident ranges at capacity | 288.34 ns |
| Concurrent hit | 8 workers sharing one key | 73.21 ns/read |
| Warm read-through | 4 KiB cached read | 85.80 ns |
| Fragmented reconstruction | 64 alternating cached/missing segments | 18.00 µs |
| Coalesced read-through | 32 identical concurrent readers | 11.17 µs |

The coalesced 32-reader case performs one 4 KiB source read; issuing those reads
directly would perform 32 calls and fetch 128 KiB.

Measured with `cargo bench --all-features --bench range_cache -- --noplot` on
commit `4e9e7baa61ede11bf70b0d246954f3187c1328d8` using `rustc 1.97.1` on macOS
26.5, an Apple M4 Pro (14 cores), and 24 GiB of memory. These numbers describe
that machine and revision; they are not cross-platform performance guarantees.

## Observability

`RangeCache::snapshot` returns a consistent view of capacity, resident bytes,
key and range counts, hits, partial hits, misses, insertions, oversized
admission rejections, and evictions. Statistics are retained across `clear`.

## Compatibility

- Rust 1.86 or newer
- Rust edition 2024
- Default features: none
- License: Apache-2.0

See [CHANGELOG.md](CHANGELOG.md) for release notes and
[CONTRIBUTING.md](CONTRIBUTING.md) for development commands.
