#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::anvil_db::{AnvilDb, AnvilDbConfig};
    use crate::common::hex;
    use crate::context::Context;
    use crate::context::SimpleContext;
    use crate::kv::{JoinedIter, TombstonePair, TryTombstoneScanner};
    use crate::logging::{debug, DefaultLogger};
    use crate::sst::block_cache::cache::{LruBlockCache, NoBlockCache};
    use crate::sst::block_cache::BlockCache;
    use crate::sst::common::SstError;
    use crate::sst::reader::{SstReader, SstScanner};
    use crate::sst::writer::{NameGenerator, SstWriteSettings, SstWriter};
    use crate::storage::blob_store::{BlobId, BlobStore, InMemoryBlobStore};
    use crate::var_int::VarInt64;
    use crate::wal::wal_wrapper::{WalWrapper, WalWrapperConfig};

    type TestOnlyContext = SimpleContext<InMemoryBlobStore, LruBlockCache, DefaultLogger>;
    type TestOnlyAnvilDb = AnvilDb<TestOnlyContext>;

    #[test]
    fn test_empty_recovery() {
        let store = InMemoryBlobStore::new();
        let jdb: TestOnlyAnvilDb =
            AnvilDb::recover(store.clone(), AnvilDbConfig::default()).unwrap();
        assert!(jdb.get(b"key").unwrap().is_none());
        jdb.close().unwrap();
        let jdb: TestOnlyAnvilDb =
            AnvilDb::recover(store.clone(), AnvilDbConfig::default()).unwrap();
        assert!(jdb.get(b"key").unwrap().is_none());
        jdb.close().unwrap();
    }

    type EasyContext = SimpleContext<InMemoryBlobStore, NoBlockCache, DefaultLogger>;

    #[test]
    fn test_simple_scan() {
        let top: usize = 500_usize;

        fn usize_to_buf(k: usize) -> Vec<u8> {
            VarInt64::try_from(k).unwrap().data_ref().to_vec()
        }

        fn write_sst(
            store: &impl BlobStore,
            pairs: Vec<(Vec<u8>, Vec<u8>)>,
            gen: &mut NameGenerator,
        ) -> BlobId {
            let write_settings = SstWriteSettings::default();
            let sst_writer = SstWriter::new(write_settings);
            let pair_iter = pairs.iter().map(|(key, value)| {
                debug!(&(), "[writing] key: {:?} value: {:?}", hex(key), hex(value));
                let ok: Result<_, ()> = Ok(TombstonePair::new(key.clone(), value.clone()));
                ok
            });
            let blob_ids = sst_writer.write_all(store, pair_iter, gen).unwrap().1;
            assert_eq!(blob_ids.len(), 1);
            blob_ids[0].clone()
        }

        fn sort_pairs(pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<(Vec<u8>, Vec<u8>)> {
            let mut sorted_pairs = pairs;
            sorted_pairs.sort_by(|a, b| a.0.cmp(&b.0));
            sorted_pairs
        }
        // Create some data.
        let alpha_pairs: Vec<_> = (0..top)
            .map(|k| {
                let key = usize_to_buf(k);
                let value = usize_to_buf(k * k);
                (key, value)
            })
            .collect();
        let beta_pairs: Vec<_> = (0..top)
            .filter_map(|k| {
                if k % 2 == 0 {
                    return None;
                }
                let key = usize_to_buf(k);
                let value = usize_to_buf(k + 1);
                Some((key, value))
            })
            .collect();
        let mut mapped_pairs: HashMap<_, _> = alpha_pairs.iter().cloned().collect();
        for (key, value) in beta_pairs.iter() {
            mapped_pairs.insert(key.clone(), value.clone()).unwrap();
        }
        let sorted_pairs: Vec<(Vec<u8>, Vec<u8>)> = mapped_pairs.into_iter().collect();
        let sorted_pairs = sort_pairs(sorted_pairs);

        fn make_scanner<'a>(
            ctx: &'a EasyContext,
            pairs: Vec<(Vec<u8>, Vec<u8>)>,
            gen: &mut NameGenerator,
        ) -> SstScanner<'a, EasyContext> {
            let pairs = sort_pairs(pairs);
            let blob_id = write_sst(ctx.blob_store_ref(), pairs, gen);
            let sst_reader = SstReader::read_only(ctx.blob_store_ref(), &blob_id).unwrap();
            let sst_scanner = sst_reader.try_scan(ctx).unwrap();
            sst_scanner
        }

        fn check_scanner(scanner: SstScanner<EasyContext>, pairs: Vec<(Vec<u8>, Vec<u8>)>) {
            let scanned_pairs = scanner
                .map(|result| {
                    let pair = result.unwrap();
                    let key = pair.key_ref();
                    let value = pair.value_ref().vec_ref().unwrap();
                    (key.to_vec(), value.to_vec())
                })
                .collect::<Vec<_>>();
            let sorted_pairs = sort_pairs(pairs);
            for (idx, (scanned_pair, expected_pair)) in
                scanned_pairs.iter().zip(sorted_pairs.iter()).enumerate()
            {
                let (k1, k2) = scanned_pair;
                debug!(&(), "{idx} scanned_pair: {:?} {:?}", hex(k1), hex(k2));
                let (e1, e2) = expected_pair;
                debug!(&(), "{idx} expected_pair: {:?} {:?}", hex(e1), hex(e2));
                assert_eq!(scanned_pair.0, expected_pair.0);
                assert_eq!(scanned_pair.1, expected_pair.1);
            }
            assert_eq!(scanned_pairs.len(), sorted_pairs.len());
        }

        let store = InMemoryBlobStore::new();
        let ctx = SimpleContext::from((
            store,
            NoBlockCache::with_capacity(0),
            DefaultLogger::default(),
        ));
        let mut gen = NameGenerator::new(0);
        let alpha_scanner = make_scanner(&ctx, alpha_pairs.clone(), &mut gen);
        check_scanner(alpha_scanner, alpha_pairs.clone());

        let alpha_scanner = make_scanner(&ctx, alpha_pairs.clone(), &mut gen);
        let beta_scanner = make_scanner(&ctx, beta_pairs.clone(), &mut gen);

        let join_scanner: JoinedIter<SstError, _, _> = JoinedIter::new(beta_scanner, alpha_scanner);
        let scanned_pairs = join_scanner
            .map(|result| {
                let pair = result.unwrap();
                let key = pair.key_ref();
                let value = pair.value_ref().vec_ref().unwrap();
                (key.to_vec(), value.to_vec())
            })
            .collect::<Vec<_>>();
        for (scanned_pair, expected_pair) in scanned_pairs.iter().zip(sorted_pairs.iter()) {
            let (k1, k2) = scanned_pair;
            debug!(&(), "scanned_pair: {:?} {:?}", hex(k1), hex(k2));
            let (e1, e2) = expected_pair;
            debug!(&(), "expected_pair: {:?} {:?}", hex(e1), hex(e2));
            assert_eq!(scanned_pair.0, expected_pair.0);
            assert_eq!(scanned_pair.1, expected_pair.1);
        }
        assert_eq!(scanned_pairs.len(), sorted_pairs.len());
    }

    #[test]
    fn test_minor_merge_recovery() {
        let top: usize = 500_usize;

        // Create some data.
        let mut keys: Vec<Vec<u8>> = Vec::new();
        let mut values_even: Vec<Vec<u8>> = Vec::new();
        let mut values_odd: Vec<Vec<u8>> = Vec::new();
        let mut odd_sorted: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut values_3: Vec<Vec<u8>> = Vec::new();
        for k in 0..top {
            let key = VarInt64::try_from(k).unwrap().data_ref().to_vec();
            keys.push(key.clone());
            let value = VarInt64::try_from(k * k).unwrap().data_ref().to_vec();
            values_even.push(value);
            let value = VarInt64::try_from(k + 1).unwrap().data_ref().to_vec();
            values_odd.push(value.clone());

            if k % 2 == 1 {
                odd_sorted.push((key.clone(), value.clone()));
            }
            let value = VarInt64::try_from(2 * k).unwrap().data_ref().to_vec();
            values_3.push(value);
        }

        odd_sorted.sort_by(|a, b| a.0.as_slice().cmp(b.0.as_slice()));

        debug!(&(), "OPEN A NEW DB");
        let store = InMemoryBlobStore::new();

        let jdb: TestOnlyAnvilDb = AnvilDb::new(store.clone()).unwrap();

        debug!(&(), "CREATE SOME DATA");

        let dummy = vec![0, 0, 0];

        debug!(&(), "SET EVENS AS VALUES_EVAN");
        for k in 0..top {
            if k % 2 != 0 {
                continue;
            }
            let key = &keys[k];
            let val = &values_even[k];
            // TODO(t/1382): Uncomment this line.
            // assert!(!jdb.update(&key, &val).is_ok());
            assert!(jdb.get(key).unwrap().is_none());
            jdb.set(key, val).unwrap();
            jdb.set(key, &dummy).unwrap();
            // TODO(t/1382): Replace the previous set operation with the following
            // commented out line:
            // assert!(jdb.update(&key, &dummy).is_ok());
            assert_eq!(jdb.get(key).unwrap(), Some(dummy.clone()));
            jdb.set(key, val).unwrap();
            // TODO(t/1382): Replace the previous set operation with the following
            // commented out line:
            // assert!(jdb.update(&key, &val).is_ok());
            assert_eq!(jdb.get(key).unwrap(), Some(val.clone()));
        }

        jdb.close().unwrap();

        debug!(&(), "CHECK VALUES SURVIVED RECOVERY");
        let jdb: TestOnlyAnvilDb =
            AnvilDb::recover(store.clone(), AnvilDbConfig::default()).unwrap();
        debug!(&(), "DATABASE LOADED AFTER RECOVERY");

        for k in 0..top {
            if k % 2 != 0 {
                continue;
            }
            let key = &keys[k];
            let val = &values_even[k];
            assert_eq!(
                jdb.get(key).unwrap(),
                Some(val.clone()),
                "[k = {k}] get {key:?} returned None via f; expected {val:?}",
            );
        }

        debug!(&(), "WRITE ODDS AS VALUES_2");
        for k in 1..top {
            if k % 2 != 1 {
                continue;
            }
            let key = &keys[k];
            let val = &values_odd[k];
            assert!(jdb.get(key).unwrap().is_none());
            jdb.set(key, val).unwrap();
            assert_eq!(jdb.get(key).unwrap(), Some(val.clone()))
        }

        jdb.close().unwrap();

        assert!(store.exists("wal.0.log").unwrap());
        assert!(store.exists("wal.1.log").unwrap());

        let (_wrapper, recovery) = WalWrapper::recover(store.clone(), WalWrapperConfig::default())
            .map_err(|e| format!("{e:?}"))
            .unwrap();
        let pair_lists = recovery.un_compacted_updates_ref();
        assert!(pair_lists.len() == 1 || pair_lists.len() == 2);
        for (blob_id, pairs) in pair_lists {
            if blob_id != "wal.1.log" {
                continue;
            }
            for (idx, pair) in pairs.iter().enumerate() {
                let want = &odd_sorted[idx];
                let key = &want.0;
                let value = &want.1;
                assert_eq!(key.as_slice(), pair.key_ref(), "{blob_id}");
                assert_eq!(Some(value), pair.value_ref().vec_ref());
            }
        }

        debug!(&(), "CHECK ALL VALUES SURVIVED RECOVERY");
        let jdb: TestOnlyAnvilDb =
            AnvilDb::recover(store.clone(), AnvilDbConfig::default()).unwrap();

        std::thread::sleep(std::time::Duration::from_secs(1));

        for k in 0..top {
            let key = &keys[k];
            let val = if k % 2 == 0 {
                &values_even[k]
            } else {
                &values_odd[k]
            };
            assert_eq!(
                jdb.get(key).unwrap(),
                Some(val.clone()),
                "[k = {k}] get {key:?} returned None via f; expected {val:?}"
            );
        }

        debug!(&(), "CHECK OVERRIDING SOME VALUES");
        for k in top / 2..top {
            // first check that it is what we expect
            let key = &keys[k];
            let val = if k % 2 == 0 {
                &values_even[k]
            } else {
                &values_odd[k]
            };
            assert_eq!(
                jdb.get(key).unwrap(),
                Some(val.clone()),
                "k: {k} :: get {key:?} returned None via f, expected {val:?}",
            );

            // override the value
            let val = &values_3[k];
            jdb.set(key, val).unwrap();
            assert_eq!(jdb.get(key).unwrap(), Some(val.clone()));
        }

        jdb.close().unwrap();
    }
}
