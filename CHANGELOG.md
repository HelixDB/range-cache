# Changelog

All notable changes to this project will be documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Expanded Criterion microbenchmarks for cache operations, contention, and
  async read-through behavior.
- Complete core and async README examples plus documented benchmark results.

### Changed

- Raised the minimum supported Rust version from 1.85 to 1.86.
- Updated canonical repository links after the project ownership transfer.
- Removed recency bookkeeping from unbounded caches and made bounded touches
  reuse stable access-keyed LRU entries.
- Bounded sparse insertion and read planning to relevant ordered ranges.
- Added direct one-gap async reads and consolidated in-flight response locking.

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

[Unreleased]: https://github.com/xav-db/range-cache/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/xav-db/range-cache/releases/tag/v0.1.0
