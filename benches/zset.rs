use std::hint::black_box;

use bytes::Bytes;
use codecrafters_redis::value::SortedSetData;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::{RngExt, SeedableRng, rngs::StdRng};

const SIZES: [usize; 3] = [1_000, 100_000, 1_000_000];
const SEED: u64 = 0x5eed;
const PICKS: usize = 1024;

fn member(i: usize) -> Bytes {
    Bytes::from(format!("m:{i}"))
}

fn entries(n: usize) -> Vec<(Bytes, f64)> {
    let mut rng = StdRng::seed_from_u64(SEED);
    (0..n)
        .map(|i| (member(i), rng.random_range(0.0..1_000_000.0)))
        .collect()
}

fn build(entries: &[(Bytes, f64)]) -> SortedSetData {
    let mut set = SortedSetData::new();
    for (m, s) in entries {
        set.add(m.clone(), *s);
    }
    set
}

fn picks(n: usize) -> Vec<Bytes> {
    let mut rng = StdRng::seed_from_u64(SEED + 1);
    (0..PICKS).map(|_| member(rng.random_range(0..n))).collect()
}

fn reads(c: &mut Criterion) {
    for n in SIZES {
        let set = build(&entries(n));
        let picks = picks(n);

        let mut i = 0;
        c.benchmark_group("rank").bench_with_input(
            BenchmarkId::from_parameter(n),
            &set,
            |b, set| {
                b.iter(|| {
                    i = (i + 1) % PICKS;
                    black_box(set.rank(black_box(&picks[i])))
                });
            },
        );

        let mut i = 0;
        c.benchmark_group("score").bench_with_input(
            BenchmarkId::from_parameter(n),
            &set,
            |b, set| {
                b.iter(|| {
                    i = (i + 1) % PICKS;
                    black_box(set.score(black_box(&picks[i])))
                });
            },
        );

        c.benchmark_group("range_head").bench_with_input(
            BenchmarkId::from_parameter(n),
            &set,
            |b, set| {
                b.iter(|| black_box(set.range(black_box(0), black_box(9))));
            },
        );

        c.benchmark_group("range_tail").bench_with_input(
            BenchmarkId::from_parameter(n),
            &set,
            |b, set| {
                b.iter(|| black_box(set.range(black_box(-10), black_box(-1))));
            },
        );
    }
}

fn writes(c: &mut Criterion) {
    for n in SIZES {
        let mut set = build(&entries(n));
        let fresh = member(n);
        let mut rng = StdRng::seed_from_u64(SEED + 2);

        c.benchmark_group("add_remove")
            .bench_function(BenchmarkId::from_parameter(n), |b| {
                b.iter(|| {
                    let score = rng.random_range(0.0..1_000_000.0);
                    black_box(set.add(fresh.clone(), black_box(score)));
                    black_box(set.remove(black_box(&fresh)));
                });
            });
    }

    let entries = entries(100_000);
    let mut group = c.benchmark_group("build");
    group.sample_size(20);
    group.bench_with_input(
        BenchmarkId::from_parameter(entries.len()),
        &entries,
        |b, entries| b.iter_with_large_drop(|| build(entries)),
    );
    group.finish();
}

criterion_group!(benches, reads, writes);
criterion_main!(benches);
