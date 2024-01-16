#![feature(async_iterator)]
#![feature(noop_waker)]
#![feature(test)]

mod anvil_db;
mod bloom_filter;
mod checksum;
mod common;
mod compactor;
mod concurrent_skip_list;
mod context;
mod helpful_macros;
mod hopscotch;
mod kv;
mod logging;
mod mem_queue;
mod mem_table;
mod os;
mod sst;
mod storage;
mod tablet;
mod var_int;
mod wal;

// test only
mod monday;
mod test_util;
mod tuesday;
mod wednesday;

use std::async_iter::AsyncIterator;
use std::pin::Pin;
use std::task::{Context, Poll};

use anvil_db::{AnvilDb, AnvilDbConfig, AnvilDbScanner};
use context::SimpleContext;
use kv::RangeSet;
use logging::DefaultLogger;
use sst::block_cache::cache::LruBlockCache;
use storage::blob_store::LocalBlobStore;

type SuperSimpleContext = SimpleContext<LocalBlobStore, LruBlockCache, DefaultLogger>;
type SuperSimpleAnvilScanner = AnvilDbScanner<SuperSimpleContext>;
type SuperSimpleAnvilDb = AnvilDb<SuperSimpleContext>;

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
        let inner: SuperSimpleAnvilDb = AnvilDb::<_>::with_config(store, config.inner)?;
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

    /// Get an element from the database asynchronously.
    ///
    /// # Arguments
    ///
    /// - key: the key to retrieve
    ///
    /// # Returns
    ///
    /// A future for a byte vector if found, or an error string.
    pub async fn async_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        self.inner.async_get(key).await
    }

    /// Set a key-value pair for the database.
    ///
    /// # Arguments
    ///
    /// - key: the key to set
    /// - value: the value to set
    ///
    /// # Returns
    ///
    /// An error String on error.
    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.inner.set(key, value)
    }

    /// Set a key-value pair for the database asynchronously.
    ///
    /// # Arguments
    ///
    /// - key: the key to set
    /// - value: the value to set
    ///
    /// # Returns
    ///
    /// An error String on error.
    pub async fn async_set(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.inner.async_set(key, value).await
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

    /// Remove a key from the database asynchronously.
    ///
    /// # Arguments
    ///
    /// - key: the key to remove
    ///
    /// # Returns
    ///
    /// An error String on error.
    pub async fn async_remove(&self, key: &[u8]) -> Result<(), String> {
        self.inner.async_remove(key).await
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

    /// Get an iterator over the database.
    ///
    /// # Returns
    ///
    /// An iterator over the database.
    pub fn try_scan(&self) -> Result<AnvilScanner, String> {
        Ok(AnvilScanner {
            inner: self.inner.try_scan()?,
        })
    }

    /// Get an asynchronous iterator over the database.
    ///
    /// # Returns
    ///
    /// An asynchronous iterator over the database.
    pub fn try_async_scan(&self) -> Result<AnvilScanner, String> {
        Ok(AnvilScanner {
            inner: self.inner.try_scan()?,
        })
    }
}

#[derive(Debug)]
pub struct AnvilScanner {
    inner: SuperSimpleAnvilScanner,
}

impl AnvilScanner {
    pub fn from(mut self, key: &[u8]) -> Self {
        self.inner = self.inner.from(key);
        self
    }

    pub fn to(mut self, key: &[u8]) -> Self {
        self.inner = self.inner.to(key);
        self
    }
}

impl Iterator for AnvilScanner {
    type Item = Result<(Vec<u8>, Vec<u8>), String>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let result = self.inner.next()?;
            let pair = match result {
                Ok(pair) => pair,
                Err(err) => {
                    return Some(Err(err));
                }
            };
            let value = if let Some(value) = pair.value_ref().as_ref() {
                value.clone()
            } else {
                continue;
            };
            let key = pair.key_ref().to_vec();
            return Some(Ok((key, value)));
        }
    }
}

impl AsyncIterator for AnvilScanner {
    type Item = Result<(Vec<u8>, Vec<u8>), String>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // TODO(t/1336): This should actually poll the inner iterator.
        Poll::Ready(self.next())
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

    /// Set the maximum size of the write-ahead log.
    ///
    /// # Arguments
    ///
    /// - max_wal_bytes: the maximum size of the write-ahead log in bytes
    ///
    /// # Returns
    ///
    /// The configuration object.
    pub fn with_max_wal_bytes(self, max_wal_bytes: usize) -> Self {
        let inner = self.inner.with_max_wal_bytes(max_wal_bytes);
        AnvilDBConfig { inner }
    }

    /// Set the maximum size of the block cache in bytes.
    /// By default, the AnvilDB will measure how much memory on the system is
    /// unused and plan to use half of it for the block cache.
    /// If the OS is not supported or if reading the memory fails, the default
    /// is set to 0.5 GiB.
    /// Using the function overrides that behavior.
    ///
    /// TODO(t/1441): The memory is not allocated up-front.
    ///
    /// # Arguments
    ///
    /// - cache_size_bytes: the maximum size of the block cache in bytes
    ///
    /// # Returns
    ///
    /// The configuration object.
    pub fn with_cache_size_bytes(self, cache_size_bytes: usize) -> Self {
        let inner = self.inner.with_cache_size_bytes(cache_size_bytes);
        AnvilDBConfig { inner }
    }
}
