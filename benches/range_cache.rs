use std::{
    hint::black_box,
    num::NonZeroUsize,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use range_cache::{CacheCapacity, RangeCache};

fn full_hits(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("full_hit");
    for size in [1_024, 65_536, 1_048_576] {
        for (policy, capacity) in [
            ("unbounded_cached_bytes", CacheCapacity::Unbounded),
            (
                "bounded_cached_bytes",
                CacheCapacity::Bounded(
                    NonZeroUsize::new(size).expect("benchmark capacity is non-zero"),
                ),
            ),
        ] {
            let cache = RangeCache::new(capacity);
            cache
                .insert(0_u8, 0..size, Bytes::from(vec![1; size]))
                .expect("benchmark insert");
            group.bench_with_input(BenchmarkId::new(policy, size), &size, |bencher, &size| {
                bencher.iter(|| {
                    black_box(
                        cache
                            .get(&0, size / 4..size * 3 / 4)
                            .expect("benchmark range"),
                    )
                });
            });
        }
    }
    group.finish();
}

fn misses(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("miss");
    for ranges in [8, 64, 512] {
        let cache = fragmented_cache(ranges);
        let start = ranges * 32;
        group.bench_with_input(
            BenchmarkId::new("resident_ranges", ranges),
            &start,
            |bencher, &start| {
                bencher
                    .iter(|| black_box(cache.get(&0, start..start + 16).expect("benchmark range")));
            },
        );
    }
    group.finish();
}

fn gap_calculation(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("gap_calculation");
    for ranges in [8, 64, 512] {
        let cache = fragmented_cache(ranges);
        let end = ranges * 32;
        group.bench_with_input(
            BenchmarkId::new("resident_ranges", ranges),
            &end,
            |bencher, &end| {
                bencher
                    .iter(|| black_box(cache.missing_ranges(&0, 0..end).expect("benchmark range")));
            },
        );
    }
    group.finish();
}

fn insertion(criterion: &mut Criterion) {
    let mut cold_group = criterion.benchmark_group("cold_insertion");
    for size in [16, 4_096, 65_536] {
        cold_group.bench_with_input(BenchmarkId::new("bytes", size), &size, |bencher, &size| {
            bencher.iter_batched_ref(
                || {
                    (
                        RangeCache::new(CacheCapacity::Unbounded),
                        Some(Bytes::from(vec![1; size])),
                    )
                },
                |(cache, payload)| {
                    let Some(payload) = payload.take() else {
                        panic!("benchmark payload is available");
                    };
                    black_box(
                        cache
                            .insert(0_u8, 0..size, payload)
                            .expect("benchmark insert"),
                    )
                },
                BatchSize::SmallInput,
            );
        });
    }
    cold_group.finish();

    let mut contained_group = criterion.benchmark_group("contained_insertion");
    for size in [16, 1_024, 4_096] {
        contained_group.bench_with_input(
            BenchmarkId::new("bytes", size),
            &size,
            |bencher, &size| {
                bencher.iter_batched_ref(
                    || {
                        let cache = RangeCache::new(CacheCapacity::Unbounded);
                        cache
                            .insert(0_u8, 0..4_096, Bytes::from(vec![1; 4_096]))
                            .expect("benchmark insert");
                        (cache, Some(Bytes::from(vec![2; size])))
                    },
                    |(cache, payload)| {
                        let Some(payload) = payload.take() else {
                            panic!("benchmark payload is available");
                        };
                        black_box(
                            cache
                                .insert(0_u8, 0..size, payload)
                                .expect("benchmark contained insert"),
                        )
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    contained_group.finish();

    let mut overlap_group = criterion.benchmark_group("overlapping_insertion");
    for ranges in [8, 64, 512] {
        let end = ranges * 32;
        overlap_group.throughput(Throughput::Bytes(
            u64::try_from(end - 16).expect("benchmark size fits u64"),
        ));
        overlap_group.bench_with_input(
            BenchmarkId::new("resident_ranges", ranges),
            &end,
            |bencher, &end| {
                bencher.iter_batched_ref(
                    || {
                        (
                            fragmented_cache(ranges),
                            Some(Bytes::from(vec![2; end - 16])),
                        )
                    },
                    |(cache, payload)| {
                        let Some(payload) = payload.take() else {
                            panic!("benchmark payload is available");
                        };
                        black_box(
                            cache
                                .insert(0_u8, 8..end - 8, payload)
                                .expect("benchmark overlap"),
                        )
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    overlap_group.finish();
}

fn sparse_insertion(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("sparse_insertion");
    for ranges in [8, 64, 512] {
        group.bench_with_input(
            BenchmarkId::new("beginning_resident_ranges", ranges),
            &ranges,
            |bencher, &ranges| {
                bencher.iter_batched_ref(
                    || {
                        (
                            fragmented_cache_from(ranges, 32),
                            Some(Bytes::from_static(&[2; 16])),
                        )
                    },
                    |(cache, payload)| {
                        let Some(payload) = payload.take() else {
                            panic!("benchmark payload is available");
                        };
                        black_box(
                            cache
                                .insert(0_u8, 0..16, payload)
                                .expect("benchmark insertion"),
                        )
                    },
                    BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("end_resident_ranges", ranges),
            &ranges,
            |bencher, &ranges| {
                let start = ranges * 32;
                bencher.iter_batched_ref(
                    || (fragmented_cache(ranges), Some(Bytes::from_static(&[2; 16]))),
                    |(cache, payload)| {
                        let Some(payload) = payload.take() else {
                            panic!("benchmark payload is available");
                        };
                        black_box(
                            cache
                                .insert(0_u8, start..start + 16, payload)
                                .expect("benchmark insertion"),
                        )
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn eviction(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("eviction");
    for ranges in [8, 64, 512] {
        group.bench_with_input(
            BenchmarkId::new("resident_ranges", ranges),
            &ranges,
            |bencher, &ranges| {
                bencher.iter_batched_ref(
                    || {
                        let cache = RangeCache::new(CacheCapacity::Bounded(
                            NonZeroUsize::new(ranges * 16).expect("benchmark capacity is non-zero"),
                        ));
                        for key in 0..ranges {
                            cache
                                .insert(key, 0..16, Bytes::from_static(&[1; 16]))
                                .expect("benchmark insert");
                        }
                        cache
                    },
                    |cache| {
                        black_box(
                            cache
                                .insert(ranges, 0..16, Bytes::from_static(&[2; 16]))
                                .expect("benchmark eviction"),
                        )
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn concurrent_hits(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("concurrent_hit");
    group.throughput(Throughput::Elements(1));
    for workers in [1, 2, 4, 8] {
        for (policy, capacity) in [
            ("unbounded", CacheCapacity::Unbounded),
            (
                "bounded",
                CacheCapacity::Bounded(
                    NonZeroUsize::new(workers * 4_096).expect("benchmark capacity is non-zero"),
                ),
            ),
        ] {
            let cache = RangeCache::new(capacity);
            for key in 0..workers {
                cache
                    .insert(key, 0..4_096, Bytes::from(vec![1; 4_096]))
                    .expect("benchmark insert");
            }

            group.bench_with_input(
                BenchmarkId::new(format!("{policy}_shared_key_workers"), workers),
                &workers,
                |bencher, &workers| {
                    bencher.iter_custom(|iterations| {
                        concurrent_hit_duration(&cache, workers, iterations, true)
                    });
                },
            );
            group.bench_with_input(
                BenchmarkId::new(format!("{policy}_independent_key_workers"), workers),
                &workers,
                |bencher, &workers| {
                    bencher.iter_custom(|iterations| {
                        concurrent_hit_duration(&cache, workers, iterations, false)
                    });
                },
            );
        }
    }
    group.finish();
}

fn fragmented_cache(ranges: usize) -> RangeCache<u8> {
    fragmented_cache_from(ranges, 0)
}

fn fragmented_cache_from(ranges: usize, offset: usize) -> RangeCache<u8> {
    let cache = RangeCache::new(CacheCapacity::Unbounded);
    for range in 0..ranges {
        let start = offset + range * 32;
        cache
            .insert(0, start..start + 16, Bytes::from_static(&[1; 16]))
            .expect("benchmark insert");
    }
    cache
}

fn concurrent_hit_duration(
    cache: &RangeCache<usize>,
    workers: usize,
    iterations: u64,
    shared_key: bool,
) -> Duration {
    let ready = Arc::new(Barrier::new(workers + 1));
    let start = Arc::new(Barrier::new(workers + 1));
    let done = Arc::new(Barrier::new(workers + 1));
    let workers_u64 = u64::try_from(workers).expect("worker count fits u64");

    thread::scope(|scope| {
        for worker in 0..workers {
            let ready = Arc::clone(&ready);
            let start = Arc::clone(&start);
            let done = Arc::clone(&done);
            let worker_index = u64::try_from(worker).expect("worker index fits u64");
            let worker_iterations =
                iterations / workers_u64 + u64::from(worker_index < iterations % workers_u64);
            scope.spawn(move || {
                ready.wait();
                start.wait();
                let key = if shared_key { 0 } else { worker };
                for _ in 0..worker_iterations {
                    black_box(
                        cache
                            .get(&key, 1_024..2_048)
                            .expect("benchmark range")
                            .expect("benchmark hit"),
                    );
                }
                done.wait();
            });
        }

        ready.wait();
        let started = Instant::now();
        start.wait();
        done.wait();
        started.elapsed()
    })
}

#[cfg(feature = "async")]
mod asynchronous {
    use std::{
        convert::Infallible,
        hint::black_box,
        num::NonZeroUsize,
        ops::Range,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use async_trait::async_trait;
    use bytes::Bytes;
    use criterion::{BatchSize, BenchmarkId, Criterion};
    use futures_util::future::join_all;
    use range_cache::{CacheCapacity, CachedReader, RangeCache, RangeReader, ReaderConfig};

    struct Source {
        data: Bytes,
        yield_before_response: bool,
        calls: AtomicUsize,
        fetched_bytes: AtomicUsize,
    }

    impl Source {
        fn immediate(data: Bytes) -> Self {
            Self {
                data,
                yield_before_response: false,
                calls: AtomicUsize::new(0),
                fetched_bytes: AtomicUsize::new(0),
            }
        }

        fn yielding(data: Bytes) -> Self {
            Self {
                data,
                yield_before_response: true,
                calls: AtomicUsize::new(0),
                fetched_bytes: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }

        fn fetched_bytes(&self) -> usize {
            self.fetched_bytes.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl RangeReader<usize> for Source {
        type Error = Infallible;

        async fn read_range(
            &self,
            _key: &usize,
            range: Range<usize>,
        ) -> Result<Bytes, Self::Error> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.fetched_bytes.fetch_add(range.len(), Ordering::Relaxed);
            if self.yield_before_response {
                tokio::task::yield_now().await;
            }
            Ok(self.data.slice(range))
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("benchmark runtime")
    }

    fn reader(
        source: Arc<Source>,
        cache: RangeCache<usize>,
        concurrency: usize,
    ) -> CachedReader<usize, Source> {
        CachedReader::new(
            source,
            cache,
            ReaderConfig::new(
                NonZeroUsize::new(concurrency).expect("benchmark concurrency is non-zero"),
            ),
        )
    }

    pub(super) fn benchmarks(criterion: &mut Criterion) {
        direct_cold_and_warm_reads(criterion);
        fragmented_reconstruction(criterion);
        coalesced_concurrent_reads(criterion);
    }

    fn direct_cold_and_warm_reads(criterion: &mut Criterion) {
        let runtime = runtime();
        let data = Bytes::from(vec![1; 4_096]);
        let direct_source = Source::immediate(data.clone());
        let warm_source = Arc::new(Source::immediate(data.clone()));
        let warm_cache = RangeCache::new(CacheCapacity::Unbounded);
        warm_cache
            .insert(0, 0..4_096, data.clone())
            .expect("benchmark insert");
        let warm_reader = reader(Arc::clone(&warm_source), warm_cache, 1);

        let mut group = criterion.benchmark_group("read_through");
        group.bench_function("direct_4096_bytes", |bencher| {
            bencher.iter(|| {
                black_box(
                    runtime
                        .block_on(direct_source.read_range(&0, 0..4_096))
                        .expect("benchmark direct read"),
                )
            });
        });
        group.bench_function("cold_4096_bytes", |bencher| {
            bencher.iter_batched_ref(
                || {
                    reader(
                        Arc::new(Source::immediate(data.clone())),
                        RangeCache::new(CacheCapacity::Unbounded),
                        1,
                    )
                },
                |reader| {
                    black_box(
                        runtime
                            .block_on(reader.read(&0, 0..4_096))
                            .expect("benchmark cold read"),
                    )
                },
                BatchSize::SmallInput,
            );
        });
        group.bench_function("partial_single_gap_4096_bytes", |bencher| {
            bencher.iter_batched_ref(
                || {
                    let cache = RangeCache::new(CacheCapacity::Unbounded);
                    cache
                        .insert(0, 0..2_048, data.slice(0..2_048))
                        .expect("benchmark insert");
                    reader(Arc::new(Source::immediate(data.clone())), cache, 1)
                },
                |reader| {
                    black_box(
                        runtime
                            .block_on(reader.read(&0, 0..4_096))
                            .expect("benchmark partial read"),
                    )
                },
                BatchSize::SmallInput,
            );
        });
        group.bench_function("warm_4096_bytes", |bencher| {
            bencher.iter(|| {
                black_box(
                    runtime
                        .block_on(warm_reader.read(&0, 0..4_096))
                        .expect("benchmark warm read"),
                )
            });
        });
        group.finish();

        assert_eq!(
            warm_source.calls(),
            0,
            "warm benchmark must not access the source"
        );
    }

    fn fragmented_reconstruction(criterion: &mut Criterion) {
        let runtime = runtime();
        let mut group = criterion.benchmark_group("fragmented_reconstruction");
        for ranges in [8, 64, 256] {
            let length = ranges * 64;
            group.bench_with_input(
                BenchmarkId::new("segments", ranges),
                &length,
                |bencher, &length| {
                    bencher.iter_batched_ref(
                        || {
                            let data = Bytes::from(vec![1; length]);
                            let cache = RangeCache::new(CacheCapacity::Unbounded);
                            for segment in (0..ranges).step_by(2) {
                                let start = segment * 64;
                                cache
                                    .insert(0, start..start + 64, data.slice(start..start + 64))
                                    .expect("benchmark insert");
                            }
                            reader(Arc::new(Source::immediate(data)), cache, 16)
                        },
                        |reader| {
                            black_box(
                                runtime
                                    .block_on(reader.read(&0, 0..length))
                                    .expect("benchmark read"),
                            )
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
        group.finish();
    }

    fn coalesced_concurrent_reads(criterion: &mut Criterion) {
        let runtime = runtime();
        let mut group = criterion.benchmark_group("coalesced_concurrent_reads");
        for waiters in [2, 8, 32] {
            assert_coalescing_metrics(&runtime, waiters);
            group.bench_with_input(
                BenchmarkId::new("waiters", waiters),
                &waiters,
                |bencher, &waiters| {
                    bencher.iter_batched_ref(
                        || {
                            reader(
                                Arc::new(Source::yielding(Bytes::from(vec![1; 4_096]))),
                                RangeCache::new(CacheCapacity::Unbounded),
                                waiters,
                            )
                        },
                        |reader| {
                            let reads = (0..waiters).map(|_| reader.read(&0, 0..4_096));
                            black_box(runtime.block_on(join_all(reads)))
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
        group.finish();
    }

    fn assert_coalescing_metrics(runtime: &tokio::runtime::Runtime, waiters: usize) {
        let source = Arc::new(Source::yielding(Bytes::from(vec![1; 4_096])));
        let reader = reader(
            Arc::clone(&source),
            RangeCache::new(CacheCapacity::Unbounded),
            waiters,
        );
        let reads = (0..waiters).map(|_| reader.read(&0, 0..4_096));
        let results = runtime.block_on(join_all(reads));
        assert!(results.into_iter().all(|result| result.is_ok()));
        assert_eq!(source.calls(), 1, "identical reads share one source call");
        assert_eq!(
            source.fetched_bytes(),
            4_096,
            "identical reads fetch one payload"
        );
    }
}

#[cfg(feature = "async")]
fn async_benchmarks(criterion: &mut Criterion) {
    asynchronous::benchmarks(criterion);
}

#[cfg(not(feature = "async"))]
fn async_benchmarks(_criterion: &mut Criterion) {}

criterion_group!(
    benches,
    full_hits,
    misses,
    gap_calculation,
    insertion,
    sparse_insertion,
    eviction,
    concurrent_hits,
    async_benchmarks
);
criterion_main!(benches);
