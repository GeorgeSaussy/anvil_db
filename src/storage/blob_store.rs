#![allow(dead_code)]
// TODO(t/1239): Stop allowing dead code.

use std::collections::HashMap;
use std::fs::{canonicalize, create_dir_all, read_dir, remove_file, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

// TODO(t/1239): Add links within the documentation.

#[derive(Debug)]
pub(crate) enum BlobStoreError {
    NotImplemented,
    AlreadyExists,
    InternalError,
    UnknownError,
    NotFound,
    ReadError(Option<String>),
    WriteError(std::io::Error),
    CreateDirError(std::io::Error),
    DeleteError(std::io::Error),
    InvalidInput(String),
}

impl BlobStoreError {
    pub(crate) fn wrap_read_error(err: std::io::Error) -> BlobStoreError {
        BlobStoreError::ReadError(Some(format!("underlying storage error: {:?}", err)))
    }
}

impl From<BlobStoreError> for String {
    fn from(value: BlobStoreError) -> Self {
        format!("FileError: {:?}", value)
    }
}

type FileRef = usize;

/// An `InMemoryIdIndex` is an `IdIndex` implementation that does not attempt
/// to persist the index to disk.
pub(crate) struct InMemoryIdIndex {
    highest_ref: usize,
    pub(crate) ref_to_name: HashMap<FileRef, String>,
    pub(crate) name_to_ref: HashMap<String, FileRef>,
}

impl InMemoryIdIndex {
    fn get_ref(&self, name: &str) -> Result<FileRef, BlobStoreError> {
        if let Some(my_ref) = self.name_to_ref.get(name) {
            Ok(*my_ref)
        } else {
            Err(BlobStoreError::NotImplemented)
        }
    }

    // creates an unnamed file
    fn add_ref(&mut self, _my_ref: FileRef) -> Result<(), BlobStoreError> {
        self.ref_to_name.insert(_my_ref, String::from(""));
        Ok(())
    }

    fn list_refs(&self) -> Result<Vec<FileRef>, BlobStoreError> {
        Ok(self.ref_to_name.keys().copied().collect())
    }

    fn add_name(&mut self, _name: &str) -> Result<FileRef, BlobStoreError> {
        let max_tries = 100;
        for _ in 0..max_tries {
            self.highest_ref += 1;
            if self.ref_to_name.contains_key(&self.highest_ref) {
                continue;
            }
            self.ref_to_name
                .insert(self.highest_ref, String::from(_name));
            self.name_to_ref
                .insert(String::from(_name), self.highest_ref);
            return Ok(self.highest_ref);
        }
        Err(BlobStoreError::InternalError)
    }

    fn delete_name(&mut self, name: &str) -> Result<(), BlobStoreError> {
        if let Some(my_ref) = self.name_to_ref.get(name) {
            self.ref_to_name.remove(my_ref);
            self.name_to_ref.remove(name);
            Ok(())
        } else {
            Err(BlobStoreError::NotFound)
        }
    }

    fn rename(&mut self, old_name: &str, new_name: &str) -> Result<(), BlobStoreError> {
        let option = self.name_to_ref.get(old_name);
        match option {
            Some(rr) => {
                let my_ref = *rr;
                self.ref_to_name.insert(my_ref, String::from(new_name));
                self.name_to_ref.remove(old_name);
                self.name_to_ref.insert(String::from(new_name), my_ref);
            }
            None => return Err(BlobStoreError::NotFound),
        }
        Ok(())
    }
}

pub(crate) trait ReadCursor {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, BlobStoreError>;
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), BlobStoreError>;
    fn offset_read(&mut self, offset: usize, buf: &mut [u8]) -> Result<usize, BlobStoreError>;
    fn skip(&mut self, offset: usize) -> Result<(), BlobStoreError>;
    fn seek_from_end(&mut self, offset: usize) -> Result<(), BlobStoreError>;
    fn size(&self) -> Result<usize, BlobStoreError>;
}

pub(crate) trait WriteCursor {
    /// The write cursor can only append to the end of the file.
    /// It fill fail if the file has already been finalized.
    fn write(&mut self, buf: &[u8]) -> Result<(), BlobStoreError>;
    fn finalize(self) -> Result<(), BlobStoreError>;
}

/// BlobStore is a trait that allows for the creation, deletion, and reading of
/// blobs. These blogs are typically files, but a blob name does not need to be
/// a valid file path.
/// Implementations of BlobStore must be thread safe.
pub(crate) trait BlobStore: Clone + Send + Sync + 'static {
    type ReadCursor: ReadCursor;
    type WriteCursor: WriteCursor;
    type BlobIter: Iterator<Item = String>;

    fn exists(&self, blob_id: &str) -> Result<bool, BlobStoreError>;
    fn read_cursor(&self, blob_id: &str) -> Result<Self::ReadCursor, BlobStoreError>;
    /// Create a new blob with the given name. If the blob already
    /// exists, the function will return an error.
    fn create_blob(&self, blob_id: &str) -> Result<Self::WriteCursor, BlobStoreError>;
    fn blob_iter(&self) -> Result<Self::BlobIter, BlobStoreError>;
    fn delete(&self, blob_id: &str) -> Result<(), BlobStoreError>;
}
struct InMemoryBlobData {
    blobs: HashMap<String, Vec<u8>>,
}

impl InMemoryBlobData {
    fn new() -> Self {
        InMemoryBlobData {
            blobs: HashMap::new(),
        }
    }
}

pub(crate) struct InMemoryReadCursor {
    blob_name: String,
    offset: usize,
    blob_store: Arc<Mutex<InMemoryBlobData>>,
}

impl ReadCursor for InMemoryReadCursor {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, BlobStoreError> {
        let file_store = self.blob_store.lock().unwrap();
        let file = file_store.blobs.get(&self.blob_name).unwrap();
        let mut i = 0;
        while i < buf.len() && self.offset + i < file.len() {
            buf[i] = file[self.offset + i];
            i += 1;
        }
        self.offset += i;
        Ok(i)
    }

    fn offset_read(&mut self, offset: usize, buf: &mut [u8]) -> Result<usize, BlobStoreError> {
        self.offset = offset;
        self.read(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), BlobStoreError> {
        let blob_store = self.blob_store.lock().unwrap();
        let blob = if let Some(blob) = blob_store.blobs.get(&self.blob_name) {
            blob
        } else {
            return Err(BlobStoreError::NotFound);
        };
        if self.offset + buf.len() > blob.len() {
            return Err(BlobStoreError::ReadError(Some(format!(
                "read_exact: offset {} + buf.len() {} = {} > blob.len() {}",
                self.offset,
                buf.len(),
                self.offset + buf.len(),
                blob.len()
            ))));
        }
        for i in 0..buf.len() {
            buf[i] = blob[self.offset + i];
        }
        self.offset += buf.len();
        Ok(())
    }

    fn skip(&mut self, offset: usize) -> Result<(), BlobStoreError> {
        let blob_store = self.blob_store.lock().unwrap();
        let blob = blob_store.blobs.get(&self.blob_name).unwrap();
        if self.offset + offset > blob.len() {
            return Err(BlobStoreError::ReadError(None));
        }
        self.offset += offset;
        Ok(())
    }

    fn seek_from_end(&mut self, offset: usize) -> Result<(), BlobStoreError> {
        let blob_store = self.blob_store.lock().unwrap();
        let blob = blob_store.blobs.get(&self.blob_name).unwrap();
        let blob_len = blob.len();
        if offset > blob_len {
            return Err(BlobStoreError::ReadError(Some(format!(
                "offset {:?} is greater than blob length {}",
                offset, blob_len
            ))));
        }
        self.offset = blob_len - offset;
        Ok(())
    }

    fn size(&self) -> Result<usize, BlobStoreError> {
        let blob_store = self.blob_store.lock().unwrap();
        if let Some(blob) = blob_store.blobs.get(&self.blob_name) {
            return Ok(blob.len());
        }
        Err(BlobStoreError::NotFound)
    }
}

pub(crate) struct InMemoryWriteCursor {
    blob_name: String,
    blob_store: Arc<Mutex<InMemoryBlobData>>,
}

impl WriteCursor for InMemoryWriteCursor {
    fn write(&mut self, buf: &[u8]) -> Result<(), BlobStoreError> {
        let mut file_store = self.blob_store.lock().unwrap();
        let file = file_store.blobs.get_mut(&self.blob_name).unwrap();
        file.extend_from_slice(buf);
        Ok(())
    }

    fn finalize(self) -> Result<(), BlobStoreError> {
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct InMemoryBlobStore {
    raw_data: Arc<Mutex<InMemoryBlobData>>,
}

impl InMemoryBlobStore {
    pub(crate) fn new() -> Self {
        let raw_data = Arc::new(Mutex::new(InMemoryBlobData::new()));

        InMemoryBlobStore { raw_data }
    }
}

impl Default for InMemoryBlobStore {
    fn default() -> Self {
        Self::new()
    }
}

impl BlobStore for InMemoryBlobStore {
    type BlobIter = Box<dyn Iterator<Item = String>>;
    type ReadCursor = InMemoryReadCursor;
    type WriteCursor = InMemoryWriteCursor;

    fn exists(&self, blob_name: &str) -> Result<bool, BlobStoreError> {
        let blob_store = self.raw_data.lock().unwrap();
        Ok(blob_store.blobs.contains_key(blob_name))
    }

    fn read_cursor(&self, blob_name: &str) -> Result<Self::ReadCursor, BlobStoreError> {
        Ok(InMemoryReadCursor {
            blob_name: blob_name.to_string(),
            offset: 0,
            blob_store: self.raw_data.clone(),
        })
    }

    fn create_blob(&self, blob_name: &str) -> Result<Self::WriteCursor, BlobStoreError> {
        let mut blob_store = self.raw_data.lock().unwrap();
        if blob_store.blobs.contains_key(blob_name) {
            return Err(BlobStoreError::AlreadyExists);
        }
        blob_store.blobs.insert(blob_name.to_string(), Vec::new());
        Ok(InMemoryWriteCursor {
            blob_name: blob_name.to_string(),
            blob_store: self.raw_data.clone(),
        })
    }

    fn delete(&self, blob_name: &str) -> Result<(), BlobStoreError> {
        let mut blob_store = self.raw_data.lock().unwrap();
        match blob_store.blobs.remove(blob_name) {
            Some(_) => Ok(()),
            None => Err(BlobStoreError::NotFound),
        }
    }

    fn blob_iter(&self) -> Result<Self::BlobIter, BlobStoreError> {
        let blob_store = self.raw_data.lock().unwrap();
        let blob_names: Vec<_> = blob_store.blobs.keys().map(|k| k.to_string()).collect();
        Ok(Box::new(blob_names.into_iter()))
    }
}

pub(crate) struct LocalReadCursor {
    file: File,
}

impl LocalReadCursor {
    pub(crate) fn new(file: File) -> Self {
        LocalReadCursor { file }
    }
}

impl ReadCursor for LocalReadCursor {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, BlobStoreError> {
        match self.file.read(buf) {
            Ok(n) => Ok(n),
            Err(err) => Err(BlobStoreError::wrap_read_error(err)),
        }
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), BlobStoreError> {
        if let Err(err) = self.file.read_exact(buf) {
            return Err(BlobStoreError::wrap_read_error(err));
        }
        Ok(())
    }

    fn offset_read(&mut self, offset: usize, buf: &mut [u8]) -> Result<usize, BlobStoreError> {
        if let Err(err) = self.file.seek(SeekFrom::Start(offset as u64)) {
            return Err(BlobStoreError::wrap_read_error(err));
        }
        match self.file.read(buf) {
            Ok(n) => Ok(n),
            Err(err) => Err(BlobStoreError::wrap_read_error(err)),
        }
    }

    fn skip(&mut self, offset: usize) -> Result<(), BlobStoreError> {
        let offset: i64 = match offset.try_into() {
            Ok(n) => n,
            Err(_) => {
                return Err(BlobStoreError::ReadError(Some(format!(
                    "offset {} cannot be converted to i64",
                    offset
                ))));
            }
        };
        match self.file.seek(SeekFrom::Current(offset)) {
            Ok(_) => Ok(()),
            Err(err) => Err(BlobStoreError::wrap_read_error(err)),
        }
    }

    fn seek_from_end(&mut self, offset: usize) -> Result<(), BlobStoreError> {
        let offset: i64 = match offset.try_into() {
            Ok(n) => n,
            Err(_) => {
                return Err(BlobStoreError::ReadError(Some(format!(
                    "offset {} cannot be converted to i64",
                    offset
                ))));
            }
        };
        match self.file.seek(SeekFrom::End(-offset)) {
            Ok(_) => Ok(()),
            Err(err) => Err(BlobStoreError::wrap_read_error(err)),
        }
    }

    fn size(&self) -> Result<usize, BlobStoreError> {
        let metadata = match self.file.metadata() {
            Ok(m) => m,
            Err(err) => {
                return Err(BlobStoreError::ReadError(Some(format!(
                    "could not get metadata: {:?}",
                    err
                ))));
            }
        };
        Ok(metadata.len() as usize)
    }
}

pub(crate) struct LocalWriteCursor {
    file: File,
}

impl LocalWriteCursor {
    pub(crate) fn new(file: File) -> Self {
        LocalWriteCursor { file }
    }
}

impl WriteCursor for LocalWriteCursor {
    fn write(&mut self, buf: &[u8]) -> Result<(), BlobStoreError> {
        match self.file.write(buf) {
            Ok(_) => Ok(()),
            Err(e) => Err(BlobStoreError::WriteError(e)),
        }
    }

    fn finalize(self) -> Result<(), BlobStoreError> {
        match self.file.sync_all() {
            Ok(_) => Ok(()),
            Err(err) => Err(BlobStoreError::WriteError(err)),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LocalBlobStore {
    working_directory: String,
}

impl LocalBlobStore {
    pub(crate) fn new(working_path: &str) -> Result<Self, BlobStoreError> {
        let dir = PathBuf::from(working_path);
        if let Err(err) = create_dir_all(&dir) {
            return Err(BlobStoreError::CreateDirError(err));
        }
        let working_directory = match canonicalize(&dir) {
            Ok(path) => {
                if let Some(s) = path.to_str() {
                    s.to_string()
                } else {
                    return Err(BlobStoreError::UnknownError);
                }
            }
            Err(err) => return Err(BlobStoreError::CreateDirError(err)),
        };
        Ok(LocalBlobStore { working_directory })
    }

    // NOTE: Originally, `blob_id` was a hash value identifying the blob.
    // However now it is just a checks that the blob name is valid.
    // It will match any name that includes only alphanumeric characters,
    // underscores, hyphens, and periods. The name cannot start with a
    // period.
    fn get_file_path(&self, blob_name: &str) -> Result<PathBuf, BlobStoreError> {
        for (i, c) in blob_name.chars().enumerate() {
            if i == 0 && c == '.' {
                return Err(BlobStoreError::InvalidInput(format!(
                    "blob name cannot start with a period: {}",
                    blob_name
                )));
            }
            if (!c.is_alphanumeric()) && c != '.' && c != '-' && c != '_' {
                return Err(BlobStoreError::InvalidInput(format!(
                    "blob name can only consist of alphanumeric characters or underscores, \
                     hyphens, or periods: offending_char={} index={} blob_name={}",
                    c, i, blob_name
                )));
            }
        }
        let path_buf = PathBuf::from(&self.working_directory);
        let file_path = path_buf.join(blob_name);
        Ok(file_path)
    }
}

impl BlobStore for LocalBlobStore {
    type BlobIter = Box<dyn Iterator<Item = String>>;
    type ReadCursor = LocalReadCursor;
    type WriteCursor = LocalWriteCursor;

    fn exists(&self, blob_id: &str) -> Result<bool, BlobStoreError> {
        let file_path = self.get_file_path(blob_id)?;
        Ok(file_path.exists())
    }

    fn read_cursor(&self, blob_id: &str) -> Result<Self::ReadCursor, BlobStoreError> {
        let file_path = self.get_file_path(blob_id)?;
        let file = match File::open(file_path) {
            Ok(f) => f,
            Err(err) => return Err(BlobStoreError::wrap_read_error(err)),
        };
        Ok(LocalReadCursor::new(file))
    }

    fn create_blob(&self, blob_id: &str) -> Result<Self::WriteCursor, BlobStoreError> {
        let file_path = self.get_file_path(blob_id)?;
        let file = match File::create(file_path) {
            Ok(f) => f,
            Err(err) => return Err(BlobStoreError::WriteError(err)),
        };
        Ok(LocalWriteCursor::new(file))
    }

    fn delete(&self, blob_id: &str) -> Result<(), BlobStoreError> {
        let file_path = self.get_file_path(blob_id)?;
        match remove_file(file_path) {
            Ok(_) => Ok(()),
            Err(err) => Err(BlobStoreError::DeleteError(err)),
        }
    }

    fn blob_iter(&self) -> Result<Self::BlobIter, BlobStoreError> {
        let dir = match read_dir(&self.working_directory) {
            Ok(dir) => dir,
            Err(err) => {
                return Err(BlobStoreError::wrap_read_error(err));
            }
        };
        let mut ret = Vec::with_capacity(8);
        for result in dir {
            match result {
                Ok(dir_entry) => {
                    let file_name = if let Some(name) = dir_entry.file_name().to_str() {
                        name.to_string()
                    } else {
                        return Err(BlobStoreError::UnknownError);
                    };
                    ret.push(file_name);
                }
                Err(err) => {
                    return Err(BlobStoreError::wrap_read_error(err));
                }
            }
        }
        Ok(Box::new(ret.into_iter()))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_util::{set_up, tear_down};

    fn check_file_store<FS: BlobStore>(fs: &FS) -> bool {
        let file_id = String::from("test_file");
        let mut wc = match fs.create_blob(&file_id) {
            Ok(c) => c,
            Err(err) => panic!("failed to create file: {:?}", err),
        };
        let data = b"Hello, world!";
        if let Err(err) = wc.write(data) {
            panic!("failed to write file: {:?}", err);
        }
        if let Err(err) = wc.finalize() {
            panic!("failed to finalize file: {:?}", err);
        }

        let mut rc = match fs.read_cursor(&file_id) {
            Ok(c) => c,
            Err(err) => panic!("failed to read file: {:?}", err),
        };
        let mut buf = [0u8; 13];
        match rc.read(buf.as_mut()) {
            Ok(n) => {
                assert!(n == 13);
                assert_eq!(data, &buf);
            }
            Err(err) => panic!("failed to read file: {:?}", err),
        }
        if let Err(err) = fs.delete(&file_id) {
            panic!("failed to delete file: {:?}", err);
        }
        true
    }

    #[test]
    fn test_in_memory_file_store() {
        let work_dir = set_up("test_in_memory_store_store");
        let fc = LocalBlobStore::new(&work_dir).unwrap();
        assert!(check_file_store(&fc));
    }

    #[test]
    fn test_file_store() {
        let work_dir = set_up("test_file_store");

        let fc = match LocalBlobStore::new(&work_dir) {
            Ok(fc) => fc,
            Err(err) => panic!("failed to create file store: {:?}", err),
        };
        assert!(check_file_store(&fc));
        tear_down(&work_dir);
    }
}
