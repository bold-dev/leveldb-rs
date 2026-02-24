use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use rusty_leveldb::{
    compressor::{self, CompressorId},
    LdbIterator, Options, WriteBatch, DB,
};
use std::hint::black_box;
use std::time::Duration; // Added for specifying measurement times

use tempfile::TempDir;

use rand::rngs::StdRng;
use rand::{distributions::Alphanumeric, Rng, RngCore, SeedableRng};

// --- Helper Functions ---

fn get_default_options() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing = true;
    opts
}

fn generate_kv(key_len: usize, val_len: usize, rng: &mut impl Rng) -> (Vec<u8>, Vec<u8>) {
    let key: String = std::iter::repeat(())
        .map(|()| rng.sample(Alphanumeric))
        .map(char::from)
        .take(key_len)
        .collect();
    let value: String = std::iter::repeat(())
        .map(|()| rng.sample(Alphanumeric))
        .map(char::from)
        .take(val_len)
        .collect();
    (key.into_bytes(), value.into_bytes())
}

// --- Synchronous DB Benchmarks ---

fn db_put_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("DB_Put");
    let key_len = 16;
    let val_len = 100;

    for &comp_id in [
        compressor::NoneCompressor::ID,
        compressor::SnappyCompressor::ID,
    ]
    .iter()
    {
        let comp_name = if comp_id == compressor::NoneCompressor::ID {
            "None"
        } else {
            "Snappy"
        };
        group.bench_with_input(
            BenchmarkId::from_parameter(comp_name),
            &comp_id,
            |b, &ci| {
                let temp_dir = TempDir::new().unwrap();
                let mut opts = get_default_options();
                opts.compressor = ci;
                let mut db = DB::open(temp_dir.path(), opts).unwrap();
                let mut rng = StdRng::seed_from_u64(123);

                b.iter_batched_ref(
                    || generate_kv(key_len, val_len, &mut rng),
                    |kv_pair| {
                        db.put(black_box(&kv_pair.0), black_box(&kv_pair.1))
                            .unwrap();
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn db_get_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("DB_Get");
    let key_len = 16;
    let val_len = 100;
    let num_items = 10_000; // Note: The provided log shows Get times in ms, suggesting DB setup might be part of each iteration.
                            // If only 'get' op is to be measured, DB setup should be outside b.iter/b.iter_batched setup.
                            // Current structure benchmarks "open + populate + get" for each sample.

    for &comp_id in [
        compressor::NoneCompressor::ID,
        compressor::SnappyCompressor::ID,
    ]
    .iter()
    {
        let comp_name = if comp_id == compressor::NoneCompressor::ID {
            "None"
        } else {
            "Snappy"
        };

        let populate_temp_dir = TempDir::new().unwrap(); // Base data for cloning
        let mut opts = get_default_options();
        opts.compressor = comp_id;

        let mut populate_rng = StdRng::seed_from_u64(42);
        let kvs: Vec<(Vec<u8>, Vec<u8>)> = (0..num_items)
            .map(|_| generate_kv(key_len, val_len, &mut populate_rng))
            .collect();

        // Pre-populate a template DB once to speed up fresh DB creation if needed,
        // though the original code creates and populates fully fresh DBs in each iter_batched setup.
        // We'll stick to the original logic of full setup per sample as provided.
        {
            let mut db = DB::open(populate_temp_dir.path(), opts.clone()).unwrap();
            for (key, value) in &kvs {
                db.put(key, value).unwrap();
            }
            db.flush().unwrap();
        }

        group.bench_function(BenchmarkId::new("Existing", comp_name), |b| {
            let mut rng = StdRng::seed_from_u64(234);
            b.iter_batched(
                || {
                    let idx = rng.gen_range(0..num_items);
                    let key = kvs[idx].0.clone();
                    let fresh_temp_dir = TempDir::new().unwrap();
                    let mut fresh_db = DB::open(fresh_temp_dir.path(), opts.clone()).unwrap();
                    for (k, v) in &kvs {
                        // Re-populating for each sample.
                        fresh_db.put(k, v).unwrap();
                    }
                    fresh_db.flush().unwrap();
                    (fresh_db, key, fresh_temp_dir)
                },
                |(mut db, key_to_get, _temp_dir)| {
                    assert!(db.get(black_box(&key_to_get)).is_some());
                },
                BatchSize::SmallInput,
            );
        });

        let non_existing_key_gen_rng = &mut StdRng::seed_from_u64(345);
        let non_existing_key = generate_kv(key_len, val_len, non_existing_key_gen_rng).0;
        group.bench_function(BenchmarkId::new("NonExisting", comp_name), |b| {
            b.iter_batched(
                || {
                    let fresh_temp_dir = TempDir::new().unwrap();
                    let mut fresh_db = DB::open(fresh_temp_dir.path(), opts.clone()).unwrap();
                    for (k, v) in &kvs {
                        // Re-populating for each sample.
                        fresh_db.put(k, v).unwrap();
                    }
                    fresh_db.flush().unwrap();
                    (fresh_db, non_existing_key.clone(), fresh_temp_dir)
                },
                |(mut db, key, _temp_dir)| {
                    assert!(db.get(black_box(&key)).is_none());
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn db_delete_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("DB_Delete");
    let key_len = 16;
    let val_len = 100;
    //let _num_items_init = 1000; // Original variable, not directly used in current logic.

    for &comp_id in [
        compressor::NoneCompressor::ID,
        compressor::SnappyCompressor::ID,
    ]
    .iter()
    {
        let comp_name = if comp_id == compressor::NoneCompressor::ID {
            "None"
        } else {
            "Snappy"
        };

        group.bench_with_input(
            BenchmarkId::from_parameter(comp_name),
            &comp_id,
            |b, &ci| {
                let mut opts = get_default_options();
                opts.compressor = ci;

                b.iter_batched(
                    || {
                        let fresh_temp_dir = TempDir::new().unwrap();
                        let mut db = DB::open(fresh_temp_dir.path(), opts.clone()).unwrap();
                        // Using thread_rng to seed StdRng for variety in keys per iteration,
                        // but still deterministic if the parent thread's RNG sequence is fixed.
                        let mut thread_rng = rand::thread_rng();
                        let mut rng = StdRng::seed_from_u64(thread_rng.next_u64());
                        let (key, value) = generate_kv(key_len, val_len, &mut rng);
                        db.put(&key, &value).unwrap();
                        db.flush().unwrap();
                        (db, key, fresh_temp_dir)
                    },
                    |(mut db, key_to_delete, _temp_dir)| {
                        db.delete(black_box(&key_to_delete)).unwrap();
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn db_write_batch_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("DB_WriteBatch");
    // Apply suggested warmup for DB_WriteBatch/Batch1000_Snappy (target time to 8.3s)
    // This will apply to all benchmarks in this group. If other batch sizes become too slow,
    // consider separate groups or more complex conditional configuration.
    group.measurement_time(Duration::from_secs_f64(8.3));

    let key_len = 16;
    let val_len = 100;

    for batch_size in [10, 100, 1000].iter() {
        for &comp_id in [
            compressor::NoneCompressor::ID,
            compressor::SnappyCompressor::ID,
        ]
        .iter()
        {
            let comp_name = if comp_id == compressor::NoneCompressor::ID {
                "None"
            } else {
                "Snappy"
            };
            let bench_id_str = format!("Batch{}_{}", batch_size, comp_name);

            group.throughput(Throughput::Elements(*batch_size as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(bench_id_str), // Use custom string for BenchmarkId
                &(*batch_size, comp_id),
                |b, &(bs, ci)| {
                    // Note: temp_dir is created for each parameter set, but DB is opened once per set.
                    // For iter_batched, if DB setup is part of the routine, it runs per sample.
                    // Here, DB is setup once, then iter_batched reuses it.
                    let temp_dir = TempDir::new().unwrap();
                    let mut opts = get_default_options();
                    opts.compressor = ci;
                    let mut db = DB::open(temp_dir.path(), opts).unwrap();
                    let mut rng = StdRng::seed_from_u64(456); // Fixed seed for key generation across iterations of iter_batched

                    // Pre-generate KVs for the batch. These KVs will be the same for every batch write in this specific benchmark setup.
                    // If you need different KVs for each batch.write, move this inside the setup closure of iter_batched.
                    let kvs_for_batch: Vec<_> = (0..bs)
                        .map(|_| generate_kv(key_len, val_len, &mut rng))
                        .collect();

                    b.iter_batched(
                        || {
                            // Setup: create the WriteBatch
                            let mut batch = WriteBatch::default();
                            for (key, value) in &kvs_for_batch {
                                // Using pre-generated KVs
                                batch.put(key, value);
                            }
                            batch
                        },
                        |batch_to_write| {
                            // Routine: write the batch
                            db.write(black_box(batch_to_write), false).unwrap();
                        },
                        BatchSize::SmallInput, // Each batch write is one operation
                    );
                    // db and temp_dir are dropped here after all iterations for this parameter set.
                },
            );
        }
    }
    group.finish();
}

fn db_iteration_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("DB_Iteration");
    // Apply suggested warmup for DB_Iteration/Items1000_Snappy (target time to 8.6s)
    group.measurement_time(Duration::from_secs_f64(8.6));

    let key_len = 16;
    let val_len = 100;

    for &num_items in [1_000, 10_000].iter() {
        for &comp_id in [
            compressor::NoneCompressor::ID,
            compressor::SnappyCompressor::ID,
        ]
        .iter()
        {
            let comp_name = if comp_id == compressor::NoneCompressor::ID {
                "None"
            } else {
                "Snappy"
            };
            let bench_id_str = format!("Items{}_{}", num_items, comp_name);

            let mut opts = get_default_options(); // Base options
            opts.compressor = comp_id;

            // Pre-generate the KVs that will be used to populate each fresh database.
            let mut populate_rng = StdRng::seed_from_u64(42); // Consistent seed for data generation
            let kvs_to_populate: Vec<(Vec<u8>, Vec<u8>)> = (0..num_items)
                .map(|_| generate_kv(key_len, val_len, &mut populate_rng))
                .collect();

            group.throughput(Throughput::Elements(num_items as u64));
            group.bench_function(BenchmarkId::from_parameter(bench_id_str), |b| {
                // iter_batched creates a fresh DB for each sample. This measures "create+populate+iterate".
                b.iter_batched(
                    || {
                        // Setup: Create and populate a fresh DB
                        let fresh_temp_dir = TempDir::new().unwrap();
                        let mut fresh_db = DB::open(fresh_temp_dir.path(), opts.clone()).unwrap();
                        for (key, value) in &kvs_to_populate {
                            fresh_db.put(key, value).unwrap();
                        }
                        fresh_db.flush().unwrap();
                        (fresh_db, fresh_temp_dir) // Pass DB and temp_dir to the routine
                    },
                    |(mut db, _temp_dir_to_drop)| {
                        // Routine: Iterate over the DB
                        let mut iter = db.new_iter().unwrap();
                        let mut count = 0;
                        while iter.advance() {
                            // Optionally black_box(iter.key()) and black_box(iter.value())
                            count += 1;
                        }
                        assert_eq!(count, num_items);
                        black_box(count); // Ensure count is used
                                          // _temp_dir_to_drop is dropped here, cleaning up the DB files for this sample
                    },
                    BatchSize::SmallInput, // Indicates setup is relatively quick per iteration
                );
            });
        }
    }
    group.finish();
}

fn db_snapshot_get_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("DB_Snapshot_Get");
    // Apply suggested warmup for DB_Snapshot_Get/* (target time to 5.8s)
    group.measurement_time(Duration::from_secs_f64(5.8));

    let key_len = 16;
    let val_len = 100;
    let num_items = 1000;

    for &comp_id in [
        compressor::NoneCompressor::ID,
        compressor::SnappyCompressor::ID,
    ]
    .iter()
    {
        let comp_name = if comp_id == compressor::NoneCompressor::ID {
            "None"
        } else {
            "Snappy"
        };

        let mut opts = get_default_options(); // Base options
        opts.compressor = comp_id;

        // Pre-generate KVs for initial population and for modifications.
        let mut rng_populate = StdRng::seed_from_u64(42); // Consistent seed
        let kvs_initial: Vec<(Vec<u8>, Vec<u8>)> = (0..num_items)
            .map(|_| generate_kv(key_len, val_len, &mut rng_populate))
            .collect();
        let (extra_key, extra_val) = generate_kv(key_len, val_len, &mut rng_populate);

        group.bench_function(BenchmarkId::from_parameter(comp_name), |b| {
            let mut rng_bench_iter = StdRng::seed_from_u64(333); // For picking keys per iteration

            // iter_batched creates a fresh DB, populates, snapshots, modifies, then gets from snapshot for each sample.
            // This benchmarks the whole sequence per sample.
            b.iter_batched(
                || {
                    // Setup for each sample
                    let fresh_temp_dir = TempDir::new().unwrap();
                    let mut fresh_db = DB::open(fresh_temp_dir.path(), opts.clone()).unwrap();

                    for (key, value) in &kvs_initial {
                        fresh_db.put(key, value).unwrap();
                    }
                    fresh_db.flush().unwrap();

                    let snapshot = fresh_db.get_snapshot();

                    fresh_db.put(&extra_key, &extra_val).unwrap();
                    if !kvs_initial.is_empty() {
                        // Avoid panic on empty kvs_initial
                        fresh_db.delete(&kvs_initial[0].0).unwrap();
                    }
                    fresh_db.flush().unwrap();

                    let idx = rng_bench_iter.gen_range(0..kvs_initial.len());
                    let key_to_get_from_snapshot = kvs_initial[idx].0.clone();

                    // Return mutable DB, snapshot, key, and temp_dir.
                    // The DB needs to be mutable if get_at requires &mut DB, otherwise &DB.
                    // rusty_leveldb::DB::get_at takes &self.
                    (fresh_db, snapshot, key_to_get_from_snapshot, fresh_temp_dir)
                },
                |(mut db_instance, snapshot_instance, key, _temp_dir_to_drop)| {
                    // Routine
                    // Use db_instance and snapshot_instance
                    assert!(db_instance
                        .get_at(&snapshot_instance, black_box(&key))
                        .unwrap()
                        .is_some());
                    // _temp_dir_to_drop is dropped here
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn db_compact_range_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("DB_CompactRange");
    let key_len = 16;
    let val_len = 100;
    let num_items = 10_000;

    for &comp_id in [
        compressor::NoneCompressor::ID,
        compressor::SnappyCompressor::ID,
    ]
    .iter()
    {
        let comp_name = if comp_id == compressor::NoneCompressor::ID {
            "None"
        } else {
            "Snappy"
        };

        group.bench_with_input(
            BenchmarkId::from_parameter(comp_name),
            &comp_id,
            |b, &ci| {
                let mut opts = get_default_options();
                opts.compressor = ci;
                // To make compaction more impactful, you might want to tune these,
                // but for now, we use defaults.
                // opts.write_buffer_size = 64 * 1024;
                // opts.max_file_size = 256 * 1024;

                let mut rng_data_gen = StdRng::seed_from_u64(42);
                let mut kvs: Vec<(Vec<u8>, Vec<u8>)> = (0..num_items)
                    .map(|_| generate_kv(key_len, val_len, &mut rng_data_gen))
                    .collect();
                kvs.sort_by(|a, b| a.0.cmp(&b.0)); // Sort for defined first/last keys

                // Clone keys for use in the routine, as kvs might be dropped if not careful with lifetimes.
                let first_key_clone = kvs.first().map(|kv| kv.0.clone());
                let last_key_clone = kvs.last().map(|kv| kv.0.clone());

                // iter_batched_ref creates a fresh DB, populates, then compacts.
                // This benchmarks "create + populate + compact" per sample.
                b.iter_batched_ref(
                    || {
                        // Setup for each sample
                        let fresh_temp_dir = TempDir::new().unwrap();
                        // Create DB with potentially modified opts if needed for compaction specific tests
                        let mut fresh_db_opts = opts.clone();
                        // Example: fresh_db_opts.max_open_files = 50; // to stress compaction
                        let mut fresh_db = DB::open(fresh_temp_dir.path(), fresh_db_opts).unwrap();

                        for (key, value) in &kvs {
                            fresh_db.put(key, value).unwrap();
                        }
                        // Create some "deletable" space
                        for i in 0..(num_items / 10) {
                            if i * 10 < kvs.len() {
                                // Ensure index is within bounds
                                fresh_db.delete(&kvs[i * 10].0).unwrap();
                            }
                        }
                        fresh_db.flush().unwrap();
                        (fresh_db, fresh_temp_dir) // Pass DB and temp_dir
                    },
                    |(db_ref, _temp_dir_to_drop)| {
                        // Routine (takes &mut (DB, TempDir))
                        if let (Some(f_key), Some(l_key)) =
                            (first_key_clone.as_ref(), last_key_clone.as_ref())
                        {
                            // db_ref is &mut DB from the tuple (fresh_db, fresh_temp_dir)
                            db_ref
                                .compact_range(black_box(f_key), black_box(l_key))
                                .unwrap();
                        }
                        // _temp_dir_to_drop is dropped here
                    },
                    BatchSize::SmallInput, // Compaction is a single, potentially long operation
                );
            },
        );
    }
    group.finish();
}

// --- Criterion Group Definitions ---

criterion_group!(
    name = benches_db_sync;
    config = Criterion::default();
    targets =
        db_put_benchmark,
        db_get_benchmark,
        db_delete_benchmark,
        db_write_batch_benchmark,
        db_iteration_benchmark,
        db_snapshot_get_benchmark,
        db_compact_range_benchmark
);

criterion_main!(benches_db_sync);
