# Changelog

All notable changes to this project will be documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project follows [Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-07-21

### Added

- Thread-safe sparse `RangeCache<K>` backed by `bytes::Bytes`.
- Explicit bounded range-level LRU and unbounded capacity policies.
- Exact gap reporting, invalidation, clearing, and state/statistics snapshots.
- Optional async read-through with a global concurrency limit and exact-request
  coalescing.
- Cancellation-safe in-flight cleanup and retryable source/short-read failures.
- Reference-model property tests, Criterion benchmarks, cross-platform CI, and
  coverage enforcement.

[0.1.0]: https://github.com/HelixDB/range-cache/releases/tag/v0.1.0

