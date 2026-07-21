# range-cache

`range-cache` is a thread-safe sparse byte-range cache for immutable sources.
It stores `bytes::Bytes` under ordered keys, merges overlapping coverage, and
can enforce a payload-byte ceiling with range-level LRU eviction.

The optional `async` feature adds a runtime-independent source trait and a
Tokio-synchronization-based read-through wrapper. It fetches only missing gaps
and coalesces identical in-flight gap requests.

```rust
use std::num::NonZeroUsize;

use bytes::Bytes;
use range_cache::{CacheCapacity, RangeCache};

let cache = RangeCache::new(CacheCapacity::Bounded(
    NonZeroUsize::new(1024).expect("capacity is non-zero"),
));

cache.insert("object", 4..8, Bytes::from_static(b"data"))?;
assert_eq!(cache.get(&"object", 4..8)?, Some(Bytes::from_static(b"data")));
# Ok::<(), range_cache::RangeError>(())
```

Mutable sources must invalidate a key before reads from a new version. The
crate intentionally does not infer source versions or provide cross-version
coherence.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

