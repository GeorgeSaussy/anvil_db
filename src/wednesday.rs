#[cfg(test)]
mod test {
    use crate::anvil_db::AnvilDb;
    use crate::context::SimpleContext;
    use crate::logging::DefaultLogger;
    use crate::sst::block_cache::cache::LruBlockCache;
    use crate::storage::blob_store::InMemoryBlobStore;

    type TestOnlyContext = SimpleContext<InMemoryBlobStore, LruBlockCache, DefaultLogger>;
    type TestOnlyAnvilDb = AnvilDb<TestOnlyContext>;

    #[test]
    fn test_anvil_db_wednesday() {
        // TODO(t/1388): This number should be bigger.
        let top = 5 * 10_000_usize;

        // create the db
        let store = InMemoryBlobStore::new();
        let jdb: TestOnlyAnvilDb = AnvilDb::new(store.clone()).unwrap();

        // set initial values
        let val = vec![1];
        for i in 0..(4 * top) {
            let idx = i as u64;
            let key = idx.to_be_bytes();
            jdb.set(&key, &val).unwrap();
        }

        // interleave writing writers and scanning
        let mut scanner = jdb.try_scan().unwrap();
        let mini = top / 5 - 1;
        for i in 0..mini {
            let idx = (i * 5) as u64;

            // set i to 2, i+1 to 0, i+2 to 3, leave i+3 as 1, and delete i+4
            jdb.set(&idx.to_be_bytes(), &[2]).unwrap();
            jdb.set(&(idx + 1).to_be_bytes(), &[0]).unwrap();
            jdb.set(&(idx + 2).to_be_bytes(), &[3]).unwrap();
            jdb.remove(&(idx + 4).to_be_bytes()).unwrap();

            // check for deleted item from previous iteration
            let mut tombstone_found = idx == 0;
            if idx != 0 {
                let pair = scanner.next().unwrap().unwrap();
                if pair.key_ref().to_vec() == (idx - 1).to_be_bytes() {
                    assert!(pair.value_ref().as_ref().is_none());
                    tombstone_found = true;
                } else {
                    assert_eq!(pair.key_ref().to_vec(), (idx).to_be_bytes());
                    assert_eq!(pair.value_ref().as_ref().unwrap().to_vec(), vec![2]);
                }
            }

            // scan over those new values
            if tombstone_found {
                let pair = scanner.next().unwrap().unwrap();
                assert_eq!(pair.key_ref().to_vec(), idx.to_be_bytes());
                let value = pair.value_ref().as_ref().unwrap().to_vec();
                assert_eq!(value.len(), 1);
                assert!(value[0] == 1 || value[0] == 2);
            }
            let pair = scanner.next().unwrap().unwrap();
            assert_eq!(pair.key_ref().to_vec(), (idx + 1).to_be_bytes());
            assert_eq!(pair.value_ref().as_ref().unwrap().to_vec(), vec![0]);
            let pair = scanner.next().unwrap().unwrap();
            assert_eq!(pair.key_ref().to_vec(), (idx + 2).to_be_bytes());
            assert_eq!(pair.value_ref().as_ref().unwrap().to_vec(), vec![3]);
            let pair = scanner.next().unwrap().unwrap();
            assert_eq!(pair.key_ref().to_vec(), (idx + 3).to_be_bytes());
            assert_eq!(pair.value_ref().as_ref().unwrap().to_vec(), vec![1]);
        }

        jdb.close().unwrap();
    }
}
