#![feature(test)]
mod anvil_db;
mod bloom_filter;
mod checksum;
mod common;
mod compactor;
mod concurrent_skip_list;
mod kv;
mod logging;
mod mem_queue;
mod mem_table;
mod sst;
mod storage;
mod tablet;
mod var_int;
mod wal;

// test only
mod monday;
mod test_util;
mod tuesday;

use anvil_db::AnvilDbConfig;
use logging::DefaultLogger;
use sst::block_cache::{NoBlockCache, SimpleStorageWrapper};
use storage::blob_store::LocalBlobStore;
use tablet::SmartTablet;

use crate::anvil_db::{AnvilDb, AnvilDbScanner};

type SuperSimpleStorageWrapper = SimpleStorageWrapper<LocalBlobStore, NoBlockCache>;
type SuperSimpleAnvilDb =
    AnvilDb<SuperSimpleStorageWrapper, SmartTablet<SuperSimpleStorageWrapper>, DefaultLogger>;

#[derive(Debug)]
pub struct AnvilDB {
    inner: SuperSimpleAnvilDb,
}

impl AnvilDB {
    /// Create a new database instance.
    ///
    /// # Arguments
    ///
    /// - dir_name: the directory containing the database files
    ///
    /// # Returns
    ///
    /// A new database instance, or an error String on error.
    pub fn new(dir_name: &str) -> Result<AnvilDB, String> {
        let store = LocalBlobStore::new(dir_name)?;
        let inner = AnvilDb::new(store)?;
        Ok(AnvilDB { inner })
    }

    /// Create a new database instance with a custom configuration.
    ///
    /// # Arguments
    ///
    /// - dir_name: the directory containing the database files
    /// - config: the configuration to use
    ///
    /// # Returns
    ///
    /// A new database instance, or an error String on error.
    pub fn with_config(dir_name: &str, config: AnvilDBConfig) -> Result<AnvilDB, String> {
        let store = LocalBlobStore::new(dir_name)?;
        let inner: SuperSimpleAnvilDb =
            AnvilDb::<_, _, DefaultLogger>::with_config(store, config.inner)?;
        Ok(AnvilDB { inner })
    }

    /// Recover the database.
    ///
    /// # Arguments
    ///
    /// - dirname: the directory containing the database files
    ///
    /// # Returns
    ///
    /// A new database instance, or an error String on error.
    pub fn recover(dir_name: &str, config: AnvilDBConfig) -> Result<AnvilDB, String> {
        let store = LocalBlobStore::new(dir_name)?;
        let inner = AnvilDb::recover(store, config.inner)?;
        Ok(AnvilDB { inner })
    }

    /// Get an element from the database.
    ///
    /// # Arguments
    ///
    /// - key: the key to retrieve
    ///
    /// # Returns
    ///
    /// A byte vector if found, or an error string.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        self.inner.get(key)
    }

    /// Set a key-value pair for the database.
    /// # Arguments
    /// - key: the key to set
    /// - value: the value to set
    /// # Returns
    /// An error String on error.
    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.inner.set(key, value)
    }

    /// Remove a key from the database.
    ///
    /// # Arguments
    ///
    /// - key: the key to remove
    ///
    /// # Returns
    ///
    /// An error String on error.
    pub fn remove(&self, key: &[u8]) -> Result<(), String> {
        self.inner.remove(key)
    }

    /// Perform a major compaction.
    ///
    /// # Returns
    ///
    /// An error String if an error occurs.
    pub fn compact(&self) -> Result<(), String> {
        self.inner.compact()
    }

    /// Close a database.
    ///
    /// # Returns
    ///
    /// An error String on error.
    pub fn close(self) -> Result<(), String> {
        self.inner.close()
    }
}

#[derive(Debug)]
pub struct AnvilScanner {
    inner: AnvilDbScanner,
}

impl AnvilScanner {
    pub fn from(self, key: &[u8]) -> Self {
        let inner = if let Ok(inner) = self.inner.from(key) {
            inner
        } else {
            // TODO(t/1380): Improve error logging.
            panic!("cannot call `from` once the iterator has started")
        };
        AnvilScanner { inner }
    }

    pub fn to(self, key: &[u8]) -> Self {
        let inner = if let Ok(inner) = self.inner.to(key) {
            inner
        } else {
            // TODO(t/1380): Improve error logging.
            panic!("cannot call `to` once the iterator has started")
        };
        AnvilScanner { inner }
    }
}

impl Iterator for AnvilScanner {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        // TODO(t/1373): Add support for scans.
        todo!()
    }
}

#[derive(Default)]
pub struct AnvilDBConfig {
    inner: AnvilDbConfig<DefaultLogger>,
}

impl AnvilDBConfig {
    /// Set the maximum number of concurrent writers.
    ///
    /// # Arguments
    ///
    /// - max_concurrent_writers: the maximum number of concurrent writers
    ///
    /// # Returns
    ///
    /// The configuration object.
    pub fn with_max_concurrent_writers(self, max_concurrent_writers: usize) -> Self {
        let inner = self
            .inner
            .with_max_concurrent_writers(max_concurrent_writers);
        AnvilDBConfig { inner }
    }

    pub fn with_max_wal_bytes(self, max_wal_bytes: usize) -> Self {
        let inner = self.inner.with_max_wal_bytes(max_wal_bytes);
        AnvilDBConfig { inner }
    }
}
