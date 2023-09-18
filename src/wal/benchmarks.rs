#[cfg(test)]
mod test {
    use std::thread::spawn;
    use std::time::Instant;
    extern crate test;
    use test::Bencher;

    use crate::logging::debug;
    use crate::storage::blob_store::{BlobStore, LocalBlobStore, WriteCursor};
    use crate::test_util::{set_up, tear_down};
    use crate::var_int::VarInt64;
    use crate::wal::wal_wrapper::{WalWrapper, WalWrapperConfig, WalWrapperConfigPreset};

    const TOP: u32 = 1_000_000; // should be 100_000_000

    fn get_key_val(key_ints: &mut [u32]) -> (Vec<u8>, Vec<u8>) {
        let little_primes = [30_011_u32, 30_013_u32, 30_029_u32];
        let big_primes = [1_000_003_u32, 1_000_033_u32, 1_000_037_u32];

        let mut key = Vec::new();
        let mut val_int = 0_u64;
        for i in 0..3 {
            key_ints[i] =
                (key_ints[i] + (i as u32) * (big_primes[i]) % little_primes[i]) % little_primes[i];
            let mut new_bytes = key_ints[i].to_be_bytes().to_vec();
            key.append(&mut new_bytes);
            val_int += key_ints[i] as u64;
        }
        let val = VarInt64::try_from(val_int).unwrap();
        (key, val.data_ref().to_vec())
    }

    #[ignore]
    #[bench]
    fn bench_writing_wal_keys_unformatted(b: &mut Bencher) {
        let work_dir = set_up("bench_writing_wal_keys_unformatted");
        let blob_store = crate::storage::blob_store::LocalBlobStore::new(&work_dir).unwrap();

        b.iter(|| {
            let start = Instant::now();
            let mut blob = blob_store.create_blob("output.txt").unwrap();
            let mut key_ints = [0_u32, 0_u32, 0_u32];
            for _ in 0..TOP {
                let (key, val) = get_key_val(&mut key_ints);
                blob.write(&key).unwrap();
                blob.write(&val).unwrap();
            }
            let end = Instant::now();
            debug!("Elapsed: {} ms", (end - start).as_millis());
        });

        tear_down(&work_dir);
    }

    fn inner_wal_write_throughput_bench<B: BlobStore + Clone + 'static>(blob_store: &B)
    where
        B::WC: Send + Sync + 'static,
    {
        let start = Instant::now();
        let wal_wrapper = WalWrapper::new(
            blob_store.clone(),
            WalWrapperConfig::from(WalWrapperConfigPreset::MultiThreadedNoSyncRequired),
        )
        .unwrap();
        let num_threads = 4;
        let mut join_handles = Vec::new();
        for thread_idx in 0..num_threads {
            let wal_wrapper = wal_wrapper.clone();
            let thread_top = TOP / num_threads;
            let thread_start = thread_idx * thread_top;
            let join_handle = spawn(move || {
                let mut key_ints = [thread_start, thread_start, thread_start];
                for _ in 0..thread_top {
                    let (key, val) = get_key_val(&mut key_ints);
                    wal_wrapper.set(key.as_ref(), &val.as_ref()).unwrap();
                }
            });
            join_handles.push(join_handle);
        }
        for join_handle in join_handles {
            join_handle.join().unwrap();
        }
        assert!(wal_wrapper.close().is_ok());
        let end = Instant::now();
        debug!("Elapsed: {} ms", (end - start).as_millis());
    }

    #[ignore]
    #[bench]
    fn bench_wal_write_throughput(b: &mut Bencher) {
        let work_dir = set_up("bench_wal_write_throughput");
        let blob_store = LocalBlobStore::new(&work_dir).unwrap();

        b.iter(|| {
            inner_wal_write_throughput_bench(&blob_store);
        });

        tear_down(&work_dir);
    }
}
