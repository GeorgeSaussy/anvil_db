#[cfg(test)]
mod test {
    use crate::anvil_db::{AnvilDb, AnvilDbConfig};
    use crate::logging::{debug, DefaultLogger};
    use crate::sst::block_cache::{NoBlockCache, SimpleStorageWrapper};
    use crate::storage::blob_store::InMemoryBlobStore;
    use crate::tablet::SmartTablet;
    use crate::var_int::VarInt64;

    #[test]
    fn test_tiny_recovery() {
        let top: usize = 10;

        debug!("OPEN A NEW DB");
        let store = InMemoryBlobStore::new();

        // TODO(t/1387): This should use the real block cache when it
        // is implemented.
        type TestOnlyStorageWrapper = SimpleStorageWrapper<InMemoryBlobStore, NoBlockCache>;
        type TestOnlyAnvilDb =
            AnvilDb<TestOnlyStorageWrapper, SmartTablet<TestOnlyStorageWrapper>, DefaultLogger>;
        let jdb: TestOnlyAnvilDb = AnvilDb::new(store.clone()).unwrap();

        debug!("CREATE SOME DATA");
        let mut keys = Vec::new();
        let mut values = Vec::new();
        for k in 0..top {
            let key: VarInt64 = VarInt64::try_from(k).unwrap();
            keys.push(key);
            let value = VarInt64::try_from(k * k).unwrap();
            values.push(value);
        }
        let dummy = vec![0, 0, 0];

        debug!("SET EVENS AS VALUES_1");
        for k in 0..top {
            if k % 2 != 0 {
                continue;
            }
            let key = &keys[k];
            let val = &values[k];
            assert!(jdb.get(key.data_ref()).unwrap().is_none());
            assert!(jdb.set(key.data_ref(), val.data_ref()).is_ok());
            assert!(jdb.set(key.data_ref(), &dummy).is_ok());
            let found = jdb.get(key.data_ref()).unwrap().unwrap();
            assert_eq!(found, dummy);
            assert!(jdb.set(key.data_ref(), val.data_ref()).is_ok());
            let getter = jdb.get(key.data_ref()).unwrap().unwrap();
            assert_eq!(getter, val.data_ref());
        }

        assert!(jdb.close().is_ok());

        debug!("CHECK VALUES SURVIVED RECOVERY");
        let jdb: TestOnlyAnvilDb = AnvilDb::recover(store, AnvilDbConfig::default()).unwrap();

        for k in 0..top {
            let key = &keys[k];
            if k % 2 != 0 {
                assert!(jdb.get(key.data_ref()).unwrap().is_none());
                continue;
            }
            let val = &values[k];
            let found = jdb.get(key.data_ref()).unwrap().unwrap();
            assert_eq!(found, val.data_ref().to_vec(), "k: {}", k);
        }

        assert!(jdb.close().is_ok());
    }

    #[test]
    fn test_anvil_db_monday() {
        // TODO(t/1388): This number should be bigger.
        let top: usize = 500_usize;

        debug!("OPEN A NEW DB");
        let store = InMemoryBlobStore::new();

        // TODO(t/1387): This should use the real block cache when it
        // is implemented.
        type TestOnlyStorageWrapper = SimpleStorageWrapper<InMemoryBlobStore, NoBlockCache>;
        type TestOnlyAnvilDb =
            AnvilDb<TestOnlyStorageWrapper, SmartTablet<TestOnlyStorageWrapper>, DefaultLogger>;
        let jdb: TestOnlyAnvilDb = AnvilDb::new(store.clone()).unwrap();

        debug!("CREATE SOME DATA");
        let mut keys = Vec::new();
        let mut values_1 = Vec::new();
        let mut values_2 = Vec::new();
        let mut values_3 = Vec::new();
        for k in 0..top {
            let key: VarInt64 = VarInt64::try_from(k).unwrap();
            keys.push(key);
            let value = VarInt64::try_from(k * k).unwrap();
            values_1.push(value);
            let value = VarInt64::try_from(k + 1).unwrap();
            values_2.push(value);
            let result = VarInt64::try_from(2 * k);
            assert!(result.is_ok(), "Could not convert {} to VarInt64.", 2 * k);
            let value = result.ok().unwrap();
            values_3.push(value);
        }
        let dummy = vec![0, 0, 0];

        debug!("SET EVENS AS VALUES_1");
        for k in 0..top {
            if k % 2 != 0 {
                continue;
            }
            let key = &keys[k];
            let val = &values_1[k];
            // TODO(t/1382): Uncomment this line.
            // assert!(!jdb.update(key.data_ref(), val.data_ref()).is_ok());
            assert!(jdb.get(key.data_ref()).unwrap().is_none());
            assert!(jdb.set(key.data_ref(), val.data_ref()).is_ok());
            assert!(jdb.set(key.data_ref(), &dummy).is_ok());
            // TODO(t/1382): Replace the previous set operation with the following
            // commented out line:
            // assert!(jdb.update(key.data_ref(), &dummy).is_ok());
            let found = jdb.get(key.data_ref()).unwrap().unwrap();
            assert_eq!(found, dummy);
            assert!(jdb.set(key.data_ref(), val.data_ref()).is_ok());
            // TODO(t/1382): Replace the previous set operation with the following
            // commented out line:
            // assert!(jdb.update(key.data_ref(), val.data_ref()).is_ok());
            let getter = jdb.get(key.data_ref()).unwrap().unwrap();
            assert_eq!(getter, val.data_ref());
        }

        assert!(jdb.close().is_ok());

        debug!("CHECK VALUES SURVIVED RECOVERY");
        let jdb: TestOnlyAnvilDb =
            AnvilDb::recover(store.clone(), AnvilDbConfig::default()).unwrap();

        for k in 0..top {
            if k % 2 != 0 {
                continue;
            }
            let key = &keys[k];
            let val = &values_1[k];
            let found = jdb.get(key.data_ref()).unwrap().unwrap();
            assert_eq!(found, val.data_ref().to_vec(), "k: {}", k);
        }

        debug!("WRITE ODDS AS VALUES_2");
        for k in 1..top {
            if k % 2 != 1 {
                continue;
            }
            let key = &keys[k];
            let val = &values_2[k];
            assert!(jdb.get(key.data_ref()).unwrap().is_none());
            assert!(jdb.set(key.data_ref(), val.data_ref()).is_ok());
            assert_eq!(jdb.get(key.data_ref()).unwrap().unwrap(), val.data_ref());
        }

        assert!(jdb.close().is_ok());

        debug!("CHECK ALL VALUES SURVIVED RECOVERY");
        let jdb: TestOnlyAnvilDb =
            AnvilDb::recover(store.clone(), AnvilDbConfig::default()).unwrap();

        for k in 0..top {
            let key = &keys[k];
            let val = if k % 2 == 0 {
                &values_1[k]
            } else {
                &values_2[k]
            };
            assert_eq!(jdb.get(key.data_ref()).unwrap().unwrap(), val.data_ref());
        }

        debug!("CHECK OVERRIDING SOME VALUES");
        for k in top / 2..top {
            // first check that it is what we expect
            let key = &keys[k];
            let val = if k % 2 == 0 {
                &values_1[k]
            } else {
                &values_2[k]
            };
            assert_eq!(jdb.get(key.data_ref()).unwrap().unwrap(), val.data_ref());

            // override the value
            let val = &values_3[k];
            assert!(jdb.set(key.data_ref(), val.data_ref()).is_ok());
            assert_eq!(jdb.get(key.data_ref()).unwrap().unwrap(), val.data_ref());
        }

        debug!("CLOSE AND RECOVER");
        assert!(jdb.close().is_ok());
        let jdb: TestOnlyAnvilDb =
            AnvilDb::recover(store.clone(), AnvilDbConfig::default()).unwrap();

        debug!("CHECK THE VALUES CAN BE DELETED");
        for k in 0..top {
            let key = &keys[k];

            assert!(jdb.get(key.data_ref()).unwrap().is_some());
            // TODO(t/1383): Uncomment the following line.
            // assert!(jdb.insert(key.data_ref(), values_2[k].data_ref()).is_err());

            assert!(jdb.remove(key.data_ref()).is_ok());
            assert!(jdb.get(key.data_ref()).unwrap().is_none());

            assert!(jdb.set(key.data_ref(), values_2[k].data_ref()).is_ok());
            // TODO(t/1382): Replace the previous set operation with the following
            // commented out line:
            // assert!(jdb.insert(key.data_ref(), values_2[k].data_ref()).is_ok());

            assert_eq!(
                jdb.get(key.data_ref()).unwrap().unwrap(),
                values_2[k].data_ref()
            );
            assert!(jdb.remove(key.data_ref()).is_ok());
            assert!(jdb.get(key.data_ref()).unwrap().is_none());
        }

        debug!("CLOSE, RECOVER, CHECK EMPTY");
        assert!(jdb.close().is_ok());
        let jdb: TestOnlyAnvilDb =
            AnvilDb::recover(store.clone(), AnvilDbConfig::default()).unwrap();

        for key in keys.iter().take(top) {
            assert!(jdb.get(key.data_ref()).unwrap().is_none());
        }

        debug!("CLEAN UP");
        assert!(jdb.close().is_ok());
    }
}
