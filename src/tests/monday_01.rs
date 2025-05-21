#[cfg(test)]
mod test {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use crate::anvil_db::{AnvilDb, AnvilDbConfig};
    use crate::context::SimpleContext;
    use crate::logging::{debug, DefaultLogger};
    use crate::sst::block_cache::cache::LruBlockCache;
    use crate::storage::blob_store::InMemoryBlobStore;
    use crate::var_int::VarInt64;

    type TestOnlyContext = SimpleContext<InMemoryBlobStore, LruBlockCache, DefaultLogger>;
    type TestOnlyAnvilDb = AnvilDb<TestOnlyContext>;

    #[test]
    fn test_tiny_recovery() {
        let top: usize = 10;

        debug!(&(), "OPEN A NEW DB");
        let store = InMemoryBlobStore::new();

        let jdb: TestOnlyAnvilDb = AnvilDb::new(store.clone()).unwrap();

        debug!(&(), "CREATE SOME DATA");
        let mut keys = Vec::new();
        let mut values = Vec::new();
        for k in 0..top {
            let key: VarInt64 = VarInt64::try_from(k).unwrap();
            keys.push(key.data_ref().to_vec());
            let value = VarInt64::try_from(k * k).unwrap();
            values.push(value);
        }
        let dummy = vec![0, 0, 0];

        debug!(&(), "SET EVENS AS VALUES_1");
        for k in 0..top {
            if k % 2 != 0 {
                continue;
            }
            let key = &keys[k];
            let val = &values[k];
            assert!(jdb.get(key).unwrap().is_none());
            assert!(jdb.set(key, val.data_ref()).is_ok());
            assert!(jdb.set(key, &dummy).is_ok());
            let found = jdb.get(key).unwrap().unwrap();
            assert_eq!(found, dummy);
            assert!(jdb.set(key, val.data_ref()).is_ok());
            let getter = jdb.get(key).unwrap().unwrap();
            assert_eq!(getter, val.data_ref());
        }

        assert!(jdb.close().is_ok());

        debug!(&(), "CHECK VALUES SURVIVED RECOVERY");
        let jdb: TestOnlyAnvilDb = AnvilDb::recover(store, AnvilDbConfig::default()).unwrap();

        for k in 0..top {
            let key = &keys[k];
            if k % 2 != 0 {
                assert!(jdb.get(key).unwrap().is_none());
                continue;
            }
            let val = &values[k];
            let found = jdb.get(key).unwrap().unwrap();
            assert_eq!(found, val.data_ref().to_vec(), "k: {k}");
        }

        assert!(jdb.close().is_ok());
    }

    fn test_anvil_db_monday_inner() -> Result<(), String> {
        // TODO(t/1388): This number should be bigger.
        let top: usize = 500_usize;

        debug!(&(), "OPEN A NEW DB");
        let store = InMemoryBlobStore::new();

        let jdb: TestOnlyAnvilDb = AnvilDb::new(store.clone())?;

        debug!(&(), "CREATE SOME DATA");
        let mut keys = Vec::new();
        let mut values_1 = Vec::new();
        let mut values_2 = Vec::new();
        let mut values_3 = Vec::new();
        for k in 0..top {
            let key: VarInt64 = VarInt64::try_from(k)?;
            keys.push(key.data_ref().to_vec());
            let value = VarInt64::try_from(k * k)?;
            values_1.push(value);
            let value = VarInt64::try_from(k + 1)?;
            values_2.push(value);
            let value = VarInt64::try_from(2 * k)?;
            values_3.push(value);
        }
        let dummy = vec![0, 0, 0];

        debug!(&(), "SET EVENS AS VALUES_1");
        for k in 0..top {
            if k % 2 != 0 {
                continue;
            }
            let key = &keys[k];
            let val = &values_1[k];
            // TODO(t/1382): Uncomment this line.
            // assert!(!jdb.update(key, val.data_ref()).is_ok());
            if jdb.get(key)?.is_some() {
                return Err("get() returned Some".to_string());
            }
            jdb.set(key, val.data_ref())?;
            jdb.set(key, &dummy)?;
            // TODO(t/1382): Replace the previous set operation with the following
            // commented out line:
            // assert!(jdb.update(key, &dummy).is_ok());
            let found_1 = jdb.get(key)?;
            let found = if let Some(f) = found_1 {
                f
            } else {
                return Err("get() returned None via found".to_string());
            };
            if found != dummy {
                return Err("get() returned wrong value".to_string());
            }
            jdb.set(key, val.data_ref())?;
            // TODO(t/1382): Replace the previous set operation with the following
            // commented out line:
            // assert!(jdb.update(key, val.data_ref()).is_ok());
            let getter_1 = jdb.get(key)?;
            let getter = if let Some(g) = getter_1 {
                g
            } else {
                return Err("get() returned None via getter".to_string());
            };
            if getter != val.data_ref() {
                return Err("get() returned wrong value".to_string());
            }
        }

        jdb.close()?;

        debug!(&(), "CHECK VALUES SURVIVED RECOVERY");
        let jdb: TestOnlyAnvilDb =
            AnvilDb::recover(store.clone(), AnvilDbConfig::default()).unwrap();
        debug!(&(), "DATABASE LOADED AFTER RECOVERY");

        for k in 0..top {
            if k % 2 != 0 {
                continue;
            }
            let key = &keys[k];
            let val = &values_1[k];
            let found = jdb.get(key).unwrap().unwrap();
            assert_eq!(found, val.data_ref().to_vec(), "k: {k}",);
        }

        debug!(&(), "WRITE ODDS AS VALUES_2");
        for k in 1..top {
            if k % 2 != 1 {
                continue;
            }
            let key = &keys[k];
            let val = &values_2[k];
            assert!(jdb.get(key).unwrap().is_none());
            assert!(jdb.set(key, val.data_ref()).is_ok());
            assert_eq!(jdb.get(key).unwrap().unwrap(), val.data_ref());
        }

        assert!(jdb.close().is_ok());

        debug!(&(), "CHECK ALL VALUES SURVIVED RECOVERY");
        let jdb: TestOnlyAnvilDb =
            AnvilDb::recover(store.clone(), AnvilDbConfig::default()).unwrap();

        std::thread::sleep(std::time::Duration::from_secs(1));

        for k in 0..top {
            let key = &keys[k];
            let val = if k % 2 == 0 {
                &values_1[k]
            } else {
                &values_2[k]
            };
            assert_eq!(jdb.get(key).unwrap().unwrap(), val.data_ref());
        }

        debug!(&(), "CHECK OVERRIDING SOME VALUES");
        for k in top / 2..top {
            // first check that it is what we expect
            let key = &keys[k];
            let val = if k % 2 == 0 {
                &values_1[k]
            } else {
                &values_2[k]
            };
            assert_eq!(jdb.get(key).unwrap().unwrap(), val.data_ref());

            // override the value
            let val = &values_3[k];
            assert!(jdb.set(key, val.data_ref()).is_ok());
            assert_eq!(jdb.get(key).unwrap().unwrap(), val.data_ref());
        }

        debug!(&(), "CLOSE AND RECOVER");
        assert!(jdb.close().is_ok());
        let jdb: TestOnlyAnvilDb =
            AnvilDb::recover(store.clone(), AnvilDbConfig::default()).unwrap();

        debug!(&(), "CHECK THE VALUES CAN BE DELETED");
        for k in 0..top {
            let key = &keys[k];

            assert!(jdb.get(key).unwrap().is_some());
            // TODO(t/1383): Uncomment the following line.
            // assert!(jdb.insert(key.data_ref(), values_2[k].data_ref()).is_err());

            assert!(jdb.remove(key).is_ok());
            assert!(jdb.get(key).unwrap().is_none());

            assert!(jdb.set(key, values_2[k].data_ref()).is_ok());
            // TODO(t/1382): Replace the previous set operation with the following
            // commented out line:
            // assert!(jdb.insert(key.data_ref(), values_2[k].data_ref()).is_ok());

            assert_eq!(jdb.get(key).unwrap().unwrap(), values_2[k].data_ref());
            assert!(jdb.remove(key).is_ok());
            assert!(jdb.get(key).unwrap().is_none());
        }

        debug!(&(), "CLOSE, RECOVER, CHECK EMPTY");
        assert!(jdb.close().is_ok());
        let jdb: TestOnlyAnvilDb =
            AnvilDb::recover(store.clone(), AnvilDbConfig::default()).unwrap();

        for key in keys.iter().take(top) {
            assert!(jdb.get(key).unwrap().is_none());
        }

        debug!(&(), "CLEAN UP");
        jdb.close()?;
        Ok(())
    }

    #[test]
    fn test_anvil_db_monday_1() {
        let result = test_anvil_db_monday_inner();
        assert!(
            result.is_ok(),
            "test_anvil_db_monday_inner failed: {result:?}"
        );
    }

    #[test]
    fn test_anvil_db_monday_2() {
        let (tx, rx) = std::sync::mpsc::channel();
        let order = std::sync::atomic::Ordering::SeqCst;
        let n_threads = 16;
        let start_signal = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::with_capacity(n_threads);
        for _ in 0..n_threads {
            let start_signal = start_signal.clone();
            let tx = tx.clone();
            let handle = std::thread::spawn(move || {
                while !start_signal.load(order) {
                    std::thread::yield_now();
                }
                tx.send(test_anvil_db_monday_inner()).unwrap();
            });
            handles.push(handle);
        }
        start_signal.store(true, order);
        let mut err_count = 0;
        for handle in handles.into_iter() {
            if let Err(_err) = handle.join() {
                err_count += 1;
            }
        }
        for _ in 0..n_threads {
            if let Ok(Err(err)) = rx.recv() {
                panic!("Thread failure: {err:?}");
            }
        }
        assert_eq!(err_count, 0, "Some threads panicked");
    }
}
