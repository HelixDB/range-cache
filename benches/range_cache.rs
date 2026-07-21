use bytes::Bytes;
use criterion::{BatchSize, Criterion, Throughput, black_box, criterion_group, criterion_main};
use range_cache::{CacheCapacity, RangeCache};

fn full_hits(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("full_hit");
    for size in [1_024, 65_536, 1_048_576] {
        let cache = RangeCache::new(CacheCapacity::Unbounded);
        cache
            .insert(0_u8, 0..size, Bytes::from(vec![1; size]))
            .expect("benchmark insert");
        group.throughput(Throughput::Bytes(
            u64::try_from(size / 2).expect("benchmark size fits u64"),
        ));
        group.bench_function(size.to_string(), |bencher| {
            bencher.iter(|| {
                black_box(
                    cache
                        .get(&0, size / 4..size * 3 / 4)
                        .expect("benchmark range"),
                )
            });
        });
    }
    group.finish();
}

fn gap_calculation(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("gap_calculation");
    for segments in [8, 64, 512] {
        let cache = RangeCache::new(CacheCapacity::Unbounded);
        for segment in 0..segments {
            let start = segment * 32;
            cache
                .insert(0_u8, start..start + 16, Bytes::from_static(&[1; 16]))
                .expect("benchmark insert");
        }
        let end = segments * 32;
        group.bench_function(segments.to_string(), |bencher| {
            bencher.iter(|| black_box(cache.missing_ranges(&0, 0..end).expect("benchmark range")));
        });
    }
    group.finish();
}

fn overlapping_insertion(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("overlapping_insertion");
    for segments in [8, 64, 512] {
        let end = segments * 32;
        group.throughput(Throughput::Bytes(
            u64::try_from(end - 16).expect("benchmark size fits u64"),
        ));
        group.bench_function(segments.to_string(), |bencher| {
            bencher.iter_batched(
                || {
                    let cache = RangeCache::new(CacheCapacity::Unbounded);
                    for segment in 0..segments {
                        let start = segment * 32;
                        cache
                            .insert(0_u8, start..start + 16, Bytes::from_static(&[1; 16]))
                            .expect("benchmark insert");
                    }
                    cache
                },
                |cache| {
                    black_box(
                        cache
                            .insert(0_u8, 8..end - 8, Bytes::from(vec![2; end - 16]))
                            .expect("benchmark overlap"),
                    )
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn eviction(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("eviction");
    for segments in [8, 64, 512] {
        group.bench_function(segments.to_string(), |bencher| {
            bencher.iter_batched(
                || {
                    let cache = RangeCache::new(CacheCapacity::Bounded(
                        std::num::NonZeroUsize::new(segments * 16)
                            .expect("benchmark capacity is non-zero"),
                    ));
                    for key in 0..segments {
                        cache
                            .insert(key, 0..16, Bytes::from_static(&[1; 16]))
                            .expect("benchmark insert");
                    }
                    cache
                },
                |cache| {
                    black_box(
                        cache
                            .insert(segments, 0..16, Bytes::from_static(&[2; 16]))
                            .expect("benchmark eviction"),
                    )
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

#[cfg(feature = "async")]
mod asynchronous {
    use std::{convert::Infallible, num::NonZeroUsize, ops::Range, sync::Arc};

    use async_trait::async_trait;
    use bytes::Bytes;
    use criterion::{BatchSize, Criterion, black_box};
    use futures_util::future::join_all;
    use range_cache::{CacheCapacity, CachedReader, RangeCache, RangeReader, ReaderConfig};

    struct Source {
        data: Bytes,
    }

    #[async_trait]
    impl RangeReader<usize> for Source {
        type Error = Infallible;

        async fn read_range(
            &self,
            _key: &usize,
            range: Range<usize>,
        ) -> Result<Bytes, Self::Error> {
            tokio::task::yield_now().await;
            Ok(self.data.slice(range))
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("benchmark runtime")
    }

    pub(super) fn benchmarks(criterion: &mut Criterion) {
        fragmented_reconstruction(criterion);
        coalesced_concurrent_reads(criterion);
    }

    fn fragmented_reconstruction(criterion: &mut Criterion) {
        let runtime = runtime();
        let mut group = criterion.benchmark_group("fragmented_reconstruction");
        for segments in [8, 64, 256] {
            let length = segments * 64;
            group.bench_function(segments.to_string(), |bencher| {
                bencher.iter_batched(
                    || {
                        let data = Bytes::from(vec![1; length]);
                        let cache = RangeCache::new(CacheCapacity::Unbounded);
                        for segment in (0..segments).step_by(2) {
                            let start = segment * 64;
                            cache
                                .insert(0, start..start + 64, data.slice(start..start + 64))
                                .expect("benchmark insert");
                        }
                        CachedReader::new(
                            Arc::new(Source { data }),
                            cache,
                            ReaderConfig::new(
                                NonZeroUsize::new(16).expect("benchmark concurrency is non-zero"),
                            ),
                        )
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
            });
        }
        group.finish();
    }

    fn coalesced_concurrent_reads(criterion: &mut Criterion) {
        let runtime = runtime();
        let mut group = criterion.benchmark_group("coalesced_concurrent_reads");
        for waiters in [2, 8, 32] {
            group.bench_function(waiters.to_string(), |bencher| {
                bencher.iter_batched(
                    || {
                        CachedReader::new(
                            Arc::new(Source {
                                data: Bytes::from(vec![1; 4_096]),
                            }),
                            RangeCache::new(CacheCapacity::Unbounded),
                            ReaderConfig::new(
                                NonZeroUsize::new(waiters)
                                    .expect("benchmark concurrency is non-zero"),
                            ),
                        )
                    },
                    |reader| {
                        let reads = (0..waiters).map(|_| reader.read(&0, 0..4_096));
                        black_box(runtime.block_on(join_all(reads)))
                    },
                    BatchSize::SmallInput,
                );
            });
        }
        group.finish();
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
    gap_calculation,
    overlapping_insertion,
    eviction,
    async_benchmarks
);
criterion_main!(benches);
