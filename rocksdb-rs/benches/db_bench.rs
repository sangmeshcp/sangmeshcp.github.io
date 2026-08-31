use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rocksdb_rs::{Options, ReadOptions, WriteOptions, WriteBatch, DB};
use tempfile::tempdir;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn bench_opts() -> Options {
    let mut o = Options::default();
    o.sync_wal = false;
    o.write_buffer_size = 32 * 1024 * 1024; // 32 MiB
    o
}

fn open_db() -> (DB, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), bench_opts()).unwrap();
    (db, dir)
}

// ─────────────────────────────────────────────────────────────────────────────
// Sequential write benchmark
// ─────────────────────────────────────────────────────────────────────────────

fn bench_sequential_writes(c: &mut Criterion) {
    let mut g = c.benchmark_group("sequential_writes");
    g.throughput(Throughput::Elements(1));

    let (db, _dir) = open_db();
    let wo = WriteOptions::default();
    let mut counter = 0u64;

    g.bench_function("put_100b_value", |b| {
        b.iter(|| {
            let key = format!("{:016}", counter);
            let val = b"x".repeat(100);
            db.put(black_box(key.as_bytes()), black_box(&val), &wo).unwrap();
            counter += 1;
        });
    });
    g.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Random read benchmark (warm cache)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_random_reads(c: &mut Criterion) {
    let (db, _dir) = open_db();
    let wo = WriteOptions::default();
    let ro = ReadOptions::default();

    // Pre-populate 10 000 keys.
    const N: u64 = 10_000;
    for i in 0..N {
        let key = format!("key-{i:08}");
        let val = format!("value-{i:08}");
        db.put(key.as_bytes(), val.as_bytes(), &wo).unwrap();
    }

    let mut g = c.benchmark_group("random_reads");
    g.throughput(Throughput::Elements(1));

    let mut rng = 0u64;
    g.bench_function("get_existing_key", |b| {
        b.iter(|| {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let i = rng % N;
            let key = format!("key-{i:08}");
            let _ = db.get(black_box(key.as_bytes()), black_box(&ro)).unwrap();
        });
    });

    g.bench_function("get_missing_key", |b| {
        b.iter(|| {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let i = rng % N + N; // guaranteed missing
            let key = format!("key-{i:08}");
            let _ = db.get(black_box(key.as_bytes()), black_box(&ro)).unwrap();
        });
    });
    g.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Batch write benchmark
// ─────────────────────────────────────────────────────────────────────────────

fn bench_batch_writes(c: &mut Criterion) {
    let mut g = c.benchmark_group("batch_writes");

    for batch_size in [10u64, 100, 1000] {
        g.throughput(Throughput::Elements(batch_size));
        g.bench_with_input(
            BenchmarkId::new("batch_put", batch_size),
            &batch_size,
            |b, &bs| {
                let (db, _dir) = open_db();
                let wo = WriteOptions::default();
                let mut counter = 0u64;
                b.iter(|| {
                    let mut batch = WriteBatch::new();
                    for _ in 0..bs {
                        let key = format!("{:016}", counter);
                        batch.put(key.as_bytes(), b"value-data-here-100bytes-padded-to-fill".as_ref());
                        counter += 1;
                    }
                    db.write(black_box(&batch), black_box(&wo)).unwrap();
                });
            },
        );
    }
    g.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Bloom filter effectiveness
// ─────────────────────────────────────────────────────────────────────────────

fn bench_bloom_filter(c: &mut Criterion) {
    let mut g = c.benchmark_group("bloom_filter");

    // With bloom filter
    {
        let mut o = bench_opts();
        o.use_bloom_filter = true;
        o.write_buffer_size = 1; // force SST flush
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path(), o).unwrap();
        let wo = WriteOptions::default();
        for i in 0u32..1000 {
            db.put(format!("present-{i:04}").as_bytes(), b"v", &wo).unwrap();
        }
        db.flush().unwrap();

        let ro = ReadOptions::default();
        let mut rng = 0u64;
        g.bench_function("bloom_on_missing_key", |b| {
            b.iter(|| {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                let key = format!("absent-{:04}", rng % 10_000);
                let _ = db.get(black_box(key.as_bytes()), black_box(&ro)).unwrap();
            });
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_sequential_writes,
    bench_random_reads,
    bench_batch_writes,
    bench_bloom_filter,
);
criterion_main!(benches);
