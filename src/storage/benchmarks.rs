#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::Write;
    use std::time::Instant;
    extern crate test;
    use test::Bencher;

    use crate::logging::debug;
    use crate::storage::blob_store::{BlobStore, WriteCursor};
    use crate::test_util::{set_up, tear_down};

    #[ignore]
    #[bench]
    fn bench_writing_file_with_native_api(b: &mut Bencher) {
        let work_dir = set_up("bench_writing_file_with_native_api");
        let path = format!("{}/output.txt", work_dir);

        b.iter(|| {
            let start = Instant::now();
            let mut file = File::create(&path).unwrap();
            let buf = vec![0_u8; 1024 * 1024 * 1024 / 2]; //  0.5 GiB
            let mut total_bytes = 0;
            let three_gib = 3 * 1024 * 1024 * 1024;
            while total_bytes < three_gib {
                let new_bytes = file.write(&buf).unwrap();
                if new_bytes == 0 {
                    panic!("no bytes were written");
                }
                total_bytes += new_bytes;
            }
            let end = Instant::now();
            debug!("Elapsed: {} ms", (end - start).as_millis());
        });

        tear_down(&work_dir);
    }

    #[bench]
    fn bench_writing_blob_with_blob_store_api(b: &mut Bencher) {
        let work_dir = set_up("anvil_db_bench_writing_blob_with_blob_store_api");
        let blob_store = crate::storage::blob_store::LocalBlobStore::new(&work_dir).unwrap();

        b.iter(|| {
            let start = Instant::now();
            let mut blob = blob_store.create_blob("output.txt").unwrap();
            let buf = vec![0_u8; 1024 * 1024 * 1024 / 2]; //  0.5 GiB
            let mut total_bytes = 0;
            let three_gib = 3 * 1024 * 1024 * 1024;
            while total_bytes < three_gib {
                blob.write(&buf).unwrap();
                total_bytes += buf.len();
            }
            let end = Instant::now();
            debug!("Elapsed: {} ms", (end - start).as_millis());
        });

        tear_down(&work_dir);
    }
}
