#[cfg(test)]
mod test {
    use crate::anvil_db::{AnvilDb, AnvilDbConfig};
    use crate::context::SimpleContext;
    use crate::logging::DefaultLogger;
    use crate::sst::block_cache::cache::LruBlockCache;
    use crate::storage::blob_store::InMemoryBlobStore;

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
}
