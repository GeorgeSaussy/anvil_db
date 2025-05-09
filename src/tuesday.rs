#[cfg(test)]
mod test {

    use crate::anvil_db::AnvilDbConfig;
    use crate::context::SimpleContext;
    use crate::helpful_macros::spawn;
    use crate::logging::debug;
    use crate::logging::DefaultLogger;
    use crate::sst::block_cache::cache::LruBlockCache;
    use crate::storage::blob_store::InMemoryBlobStore;
    use crate::var_int::VarInt64;
    use crate::AnvilDb;

    type TestOnlyContext = SimpleContext<InMemoryBlobStore, LruBlockCache, DefaultLogger>;
    type TestOnlyAnvilDb = AnvilDb<TestOnlyContext>;

    /// Set even key indexes.
    fn set_evens_as_value_1(
        t: usize,
        n_threads: usize,
        top: usize,
        jdb: &TestOnlyAnvilDb,
        keys: &[VarInt64],
        values_1: &[VarInt64],
    ) -> Result<(), String> {
        let diff = top / n_threads;
        for k in (t * diff)..((t + 1) * diff) {
            if k % 2 != 0 {
                continue;
            }
            let key = &keys[k];
            let val = &values_1[k];
            assert!(jdb.get(key.data_ref()).unwrap().is_none());
            assert!(jdb.set(key.data_ref(), val.data_ref()).is_ok());
            assert!(jdb.get(key.data_ref()).unwrap().is_some());
        }
        Ok(())
    }

    /// Check that even keys survived recovery.
    fn check_even_value_1_survived(
        t: usize,
        n_threads: usize,
        top: usize,
        jdb: &TestOnlyAnvilDb,
        keys: &[VarInt64],
        values_1: &[VarInt64],
    ) -> Result<(), String> {
        let diff = top / n_threads;
        for k in (t * diff)..((t + 1) * diff) {
            if k % 2 != 0 {
                continue;
            }
            let key = &keys[k];
            let val = &values_1[k];
            assert_eq!(jdb.get(key.data_ref()).unwrap().unwrap(), val.data_ref());
        }
        Ok(())
    }

    /// Set odd key indexes.
    fn set_odds_as_value2(
        t: usize,
        n_threads: usize,
        top: usize,
        jdb: &TestOnlyAnvilDb,
        keys: &[VarInt64],
        values_2: &[VarInt64],
    ) -> Result<(), String> {
        let diff = top / n_threads;
        for k in (t * diff)..((t + 1) * diff) {
            if k % 2 != 1 {
                continue;
            }
            let key = &keys[k];
            let val = &values_2[k];
            assert!(jdb.get(key.data_ref()).unwrap().is_none());
            assert!(jdb.set(key.data_ref(), val.data_ref()).is_ok());
            assert_eq!(jdb.get(key.data_ref()).unwrap().unwrap(), val.data_ref());
        }
        Ok(())
    }

    /// Check all values survived.
    fn check_all_values_survived(
        t: usize,
        n_threads: usize,
        top: usize,
        jdb: &TestOnlyAnvilDb,
        keys: &[VarInt64],
        values_1: &[VarInt64],
        values_2: &[VarInt64],
    ) -> Result<(), String> {
        let diff = top / n_threads;
        for k in (t * diff)..((t + 1) * diff) {
            let key = &keys[k];
            let val = if k % 2 == 0 {
                &values_1[k]
            } else {
                &values_2[k]
            };
            assert_eq!(jdb.get(key.data_ref()).unwrap().unwrap(), val.data_ref());
        }
        Ok(())
    }

    /// Check some values can be overridden.
    #[allow(clippy::too_many_arguments)]
    fn check_overriding_some_values(
        t: usize,
        n_threads: usize,
        top: usize,
        jdb: &TestOnlyAnvilDb,
        keys: &[VarInt64],
        values_1: &[VarInt64],
        values_2: &[VarInt64],
        values_3: &[VarInt64],
    ) -> Result<(), String> {
        let start = top / 2;
        let diff = start / n_threads;
        for k in (start + t * diff)..(start + (t + 1) * diff) {
            let key = keys[k].clone();
            let val = if k % 2 == 0 {
                &values_1[k]
            } else {
                &values_2[k]
            };
            assert_eq!(jdb.get(key.data_ref()).unwrap().unwrap(), val.data_ref());

            let val = &values_3[k];
            assert!(jdb.set(key.data_ref(), val.data_ref()).is_ok());
            assert_eq!(jdb.get(key.data_ref()).unwrap().unwrap(), val.data_ref());
        }
        Ok(())
    }

    /// Check that values can be deleted.
    fn check_values_can_be_deleted(
        t: usize,
        n_threads: usize,
        top: usize,
        jdb: &TestOnlyAnvilDb,
        keys: &[VarInt64],
    ) -> Result<(), String> {
        let diff = top / n_threads;
        for key in keys.iter().take((t + 1) * diff).skip(t * diff) {
            assert!(jdb.get(key.data_ref()).unwrap().is_some());
            assert!(jdb.remove(key.data_ref()).is_ok());
            assert!(jdb.get(key.data_ref()).unwrap().is_none());
        }
        Ok(())
    }

    /// Check that the database is empty.
    fn check_empty(
        t: usize,
        n_threads: usize,
        top: usize,
        jdb: &TestOnlyAnvilDb,
        keys: &[VarInt64],
    ) -> Result<(), String> {
        let diff = top / n_threads;
        for key in keys.iter().take((t + 1) * diff).skip(t * diff) {
            assert!(jdb.get(key.data_ref()).unwrap().is_none());
        }
        Ok(())
    }

    #[test]
    fn test_anvil_db_tuesday() {
        // TODO(t/1388): This number should be bigger.
        let top = 500_usize;
        let n_threads = 4;

        debug!(&(), "OPEN A NEW DB");
        let store = InMemoryBlobStore::new();
        let jdb: TestOnlyAnvilDb = AnvilDb::new(store.clone()).unwrap();

        debug!(&(), "CREATE SOME DATA");
        let mut keys = Vec::with_capacity(top);
        let mut values_1 = Vec::with_capacity(top);
        let mut values_2 = Vec::with_capacity(top);
        let mut values_3 = Vec::with_capacity(top);
        for k in 0..top {
            keys.push(VarInt64::try_from(k).unwrap());
            values_1.push(VarInt64::try_from(k * k).unwrap());
            values_2.push(VarInt64::try_from(k + 1).unwrap());
            values_3.push(VarInt64::try_from(2 * k).unwrap());
        }

        debug!(&(), "SET EVENS AS VALUES_1");
        let mut children = Vec::new();
        for t in 0..n_threads {
            let p_jdb = jdb.clone();
            let p_keys = keys.clone();
            let p_values_1 = values_1.clone();
            children.push(spawn!(move || {
                set_evens_as_value_1(t, n_threads, top, &p_jdb, &p_keys, &p_values_1)
            }));
        }
        for child in children {
            let result = child.join();
            if result.is_err() {
                panic!();
            }
        }
        let result = jdb.close();
        assert!(result.is_ok());

        debug!(&(), "CHECK VALUES SURVIVED RECOVERY");
        let jdb: TestOnlyAnvilDb =
            AnvilDb::recover(store.clone(), AnvilDbConfig::default()).unwrap();
        let mut children = Vec::with_capacity(n_threads);
        for t in 0..n_threads {
            let p_jdb = jdb.clone();
            let p_keys = keys.clone();
            let p_values_1 = values_1.clone();
            children.push(spawn!(move || {
                check_even_value_1_survived(t, n_threads, top, &p_jdb, &p_keys, &p_values_1)
            }));
        }
        for child in children {
            assert!(child.join().is_ok());
        }

        debug!(&(), "WRITE ODDS AS VALUES_2");
        let mut children = Vec::with_capacity(n_threads);
        for t in 0..n_threads {
            let p_jdb = jdb.clone();
            let p_keys = keys.clone();
            let p_values_2 = values_2.clone();
            children.push(spawn!(move || {
                set_odds_as_value2(t, n_threads, top, &p_jdb, &p_keys, &p_values_2)
            }));
        }
        for child in children {
            assert!(child.join().is_ok());
        }
        assert!(jdb.close().is_ok());

        debug!(&(), "CHECK ALL VALUES SURVIVED RECOVERY");
        let jdb: TestOnlyAnvilDb =
            AnvilDb::recover(store.clone(), AnvilDbConfig::default()).unwrap();
        let mut children = Vec::with_capacity(n_threads);
        for t in 0..n_threads {
            let p_jdb = jdb.clone();
            let p_keys = keys.clone();
            let p_values_1 = values_1.clone();
            let p_values_2 = values_2.clone();
            children.push(spawn!(move || {
                check_all_values_survived(
                    t,
                    n_threads,
                    top,
                    &p_jdb,
                    &p_keys,
                    &p_values_1,
                    &p_values_2,
                )
            }));
        }
        for child in children {
            assert!(child.join().is_ok());
        }

        debug!(&(), "CHECK OVERRIDING SOME VALUES");
        let mut children = Vec::new();
        for t in 0..n_threads {
            let p_jdb = jdb.clone();
            let p_keys = keys.clone();
            let p_values_1 = values_1.clone();
            let p_values_2 = values_2.clone();
            let p_values_3 = values_3.clone();
            children.push(spawn!(move || {
                check_overriding_some_values(
                    t,
                    n_threads,
                    top,
                    &p_jdb,
                    &p_keys,
                    &p_values_1,
                    &p_values_2,
                    &p_values_3,
                )
            }));
        }
        for child in children {
            assert!(child.join().is_ok());
        }

        debug!(&(), "CLOSE AND RECOVER");
        assert!(jdb.close().is_ok());
        let jdb: TestOnlyAnvilDb =
            AnvilDb::recover(store.clone(), AnvilDbConfig::default()).unwrap();

        debug!(&(), "CHECK THE VALUES CAN BE DELETED");
        let mut children = Vec::with_capacity(n_threads);
        for t in 0..n_threads {
            let p_jdb = jdb.clone();
            let p_keys = keys.clone();
            children.push(spawn!(move || {
                check_values_can_be_deleted(t, n_threads, top, &p_jdb, &p_keys)
            }));
        }
        for child in children {
            let result = child.join();
            if result.is_err() {
                panic!();
            }
        }

        debug!(&(), "CLOSE, RECOVER, CHECK EMPTY");
        assert!(jdb.close().is_ok());
        let jdb: TestOnlyAnvilDb = AnvilDb::recover(store, AnvilDbConfig::default()).unwrap();
        let mut children = Vec::with_capacity(n_threads);
        for t in 0..n_threads {
            let p_jdb = jdb.clone();
            let p_keys = keys.clone();
            children.push(spawn!(move || {
                check_empty(t, n_threads, top, &p_jdb, &p_keys)
            }));
        }
        for child in children {
            assert!(child.join().is_ok());
        }

        debug!(&(), "CLEAN UP");
        assert!(jdb.close().is_ok());
    }
}
