use std::fmt::Debug;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{sleep, spawn};
use std::time::Duration;

use crate::storage::blob_store::WriteCursor;
use crate::wal::wal_entry::{Checksum, WalEntry, WalEntryBlock, WalError};

// TODO(t/1336): Consider using async/await instead of channels.

pub(crate) const INIT_WAL_CHECKSUM: Checksum = 1337;

#[derive(Debug)]
enum WalRequest {
    Write(WalEntry),
    Close,
    FlushCallback,
}

struct WalRequestWrapper {
    request: WalRequest,
    result_tx: Sender<Result<(), WalError>>,
}

#[derive(Debug)]
enum FlushAction {
    ReturnOkBeforeFlush,
    FlushImmediately,
    WaitForFlush,
}

type ResultSender = Sender<Result<(), WalError>>;
#[derive(Debug)]
struct WalWriteHandler<WC: WriteCursor> {
    blob_writer: WC,
    request_rx: Receiver<WalRequestWrapper>,
    entry_buf: Vec<(WalEntry, Option<ResultSender>)>,
    prev_checksum: Checksum,
    max_wal_size_bytes: usize,
    total_bytes_written: usize,
    flush_action: FlushAction,
    rotation_requested: bool,
}

impl<WC: WriteCursor> WalWriteHandler<WC> {
    fn new(
        write_cursor: WC,
        request_rx: Receiver<WalRequestWrapper>,
        max_wal_size_bytes: usize,
        flush_action: FlushAction,
    ) -> Self {
        let blob_writer = write_cursor;
        let entry_buf = Vec::new();
        WalWriteHandler {
            blob_writer,
            request_rx,
            entry_buf,
            prev_checksum: INIT_WAL_CHECKSUM,
            max_wal_size_bytes,
            total_bytes_written: 0,
            flush_action,
            rotation_requested: false,
        }
    }

    fn run(mut self) {
        while let Ok(WalRequestWrapper { request, result_tx }) = self.request_rx.recv() {
            match request {
                WalRequest::Write(entry) => {
                    self.append_entry(entry, Some(result_tx));
                }
                WalRequest::Close => {
                    result_tx.send(self.close()).unwrap();
                    break;
                }
                WalRequest::FlushCallback => {
                    result_tx.send(self.flush()).unwrap();
                }
            }
        }
    }

    fn append_entry(
        &mut self,
        entry: WalEntry,
        wal_entry_tx: Option<Sender<Result<(), WalError>>>,
    ) {
        match self.flush_action {
            FlushAction::ReturnOkBeforeFlush => {
                self.entry_buf.push((entry, None));
                if let Some(tx) = wal_entry_tx {
                    tx.send(Ok(())).unwrap();
                }
            }
            FlushAction::FlushImmediately => {
                self.entry_buf.push((entry, wal_entry_tx));
                let _ = self.flush();
            }
            FlushAction::WaitForFlush => {
                self.entry_buf.push((entry, wal_entry_tx));
            }
        }
    }

    fn flush(&mut self) -> Result<(), WalError> {
        let (entries, result_tx): (Vec<WalEntry>, Vec<Option<ResultSender>>) =
            self.entry_buf.drain(..).unzip();
        let block = WalEntryBlock::new(entries);
        let block_data = match block.write(&mut self.blob_writer, self.prev_checksum) {
            Ok(block_data) => block_data,
            Err(err) => {
                // TODO(t/1348): Crash and recover this WAL.
                let _: Vec<()> = result_tx
                    .into_iter()
                    .map(|tx| {
                        if let Some(tx) = tx {
                            tx.send(Err(err.clone())).unwrap();
                        }
                    })
                    .collect();
                return Err(err);
            }
        };
        self.prev_checksum = block_data.checksum;
        self.total_bytes_written += block_data.bytes_written;
        for tx in result_tx.into_iter().flatten() {
            if !self.rotation_requested && self.total_bytes_written >= self.max_wal_size_bytes {
                self.rotation_requested = true;
                tx.send(Err(WalError::RotationRequired)).unwrap();
                continue;
            }
            tx.send(Ok(())).unwrap();
        }
        Ok(())
    }

    fn close(mut self) -> Result<(), WalError> {
        let result = self.flush();
        match self.blob_writer.finalize() {
            Ok(_) => result,
            Err(err) => match result {
                Ok(_) => Err(WalError::BlobStoreError(format!("{:?}", err))),
                Err(err) => Err(err),
            },
        }
    }
}

#[derive(Debug)]
enum MultiWalWriter<WC: WriteCursor> {
    MultiThreaded {
        request_tx: Sender<WalRequestWrapper>,
        // TODO(t/1374): Allow users to configure the DB to only flush
        // when told to do so.
        #[allow(dead_code)]
        wait_for_flush: bool,
    },
    SingleThreaded {
        // This is needed in case WC is not clonable.
        handler: Arc<Mutex<Option<WalWriteHandler<WC>>>>,
    },
}

impl<WC: WriteCursor> Clone for MultiWalWriter<WC> {
    fn clone(&self) -> Self {
        match self {
            MultiWalWriter::MultiThreaded {
                request_tx,
                wait_for_flush,
            } => MultiWalWriter::MultiThreaded {
                request_tx: request_tx.clone(),
                wait_for_flush: *wait_for_flush,
            },
            MultiWalWriter::SingleThreaded { handler } => MultiWalWriter::SingleThreaded {
                handler: handler.clone(),
            },
        }
    }
}

impl<WC: WriteCursor> MultiWalWriter<WC>
where
    WC: Send + 'static,
{
    fn new(
        blob_writer: WC,
        max_concurrent_writes: Option<usize>,
        return_ok_before_flush: bool,
        max_flush_wait_millis: Option<u64>,
        max_wal_size_bytes: usize,
    ) -> Self {
        let flush_action = if return_ok_before_flush {
            FlushAction::ReturnOkBeforeFlush
        } else if let Some(max_flush_wait_millis) = max_flush_wait_millis {
            if max_flush_wait_millis == 0 {
                FlushAction::FlushImmediately
            } else {
                FlushAction::WaitForFlush
            }
        } else {
            FlushAction::FlushImmediately
        };

        let (request_tx, request_rx) = channel();
        let handler =
            WalWriteHandler::new(blob_writer, request_rx, max_wal_size_bytes, flush_action);

        if let Some(max_concurrent_writes) = max_concurrent_writes {
            if max_concurrent_writes == 1 {
                let handler = Arc::new(Mutex::new(Some(handler)));
                return MultiWalWriter::SingleThreaded { handler };
            }
        }

        // TODO(t/1395): This should be handled by a fiber scheduler.
        if let Some(max_flush_wait_millis) = max_flush_wait_millis {
            let request_tx_2 = request_tx.clone();
            spawn(move || {
                MultiWalWriter::<WC>::handle_maintenance_requests(
                    max_flush_wait_millis,
                    request_tx_2,
                );
            });
        }

        spawn(move || {
            handler.run();
        });
        // TODO(t/1374): `wait_for_flush` should be configurable.
        let wait_for_flush = true;
        MultiWalWriter::MultiThreaded {
            request_tx,
            wait_for_flush,
        }
    }

    fn write(&self, entry: WalEntry) -> Result<(), WalError> {
        match self {
            MultiWalWriter::MultiThreaded { request_tx, .. } => {
                let (result_tx, result_rx) = channel();
                request_tx
                    .send(WalRequestWrapper {
                        request: WalRequest::Write(entry),
                        result_tx,
                    })
                    .unwrap();
                result_rx.recv().unwrap()
            }
            MultiWalWriter::SingleThreaded { handler } => {
                let mut handler = handler.lock().unwrap();
                if let Some(handler) = handler.as_mut() {
                    let (result_tx, result_rx) = channel();
                    // TODO(t/1395): It should be possible to do this without a
                    // channel through a send-and-flush function.
                    handler.append_entry(entry, Some(result_tx));
                    handler.flush()?;
                    let result = result_rx.recv().unwrap();
                    result?;
                    return Ok(());
                }
                Err(WalError::Closed)
            }
        }
    }

    fn close(self) -> Result<(), WalError> {
        match self {
            MultiWalWriter::MultiThreaded { request_tx, .. } => {
                let (result_tx, result_rx) = channel();
                request_tx
                    .send(WalRequestWrapper {
                        request: WalRequest::Close,
                        result_tx,
                    })
                    .unwrap();
                result_rx.recv().unwrap()
            }
            MultiWalWriter::SingleThreaded { handler } => {
                let mut handler = handler.lock().unwrap();
                if let Some(inner) = handler.take() {
                    inner.close()?;
                    return Ok(());
                }
                Err(WalError::Closed)
            }
        }
    }

    fn handle_maintenance_requests(
        max_flush_wait_millis: u64,
        request_tx: Sender<WalRequestWrapper>,
    ) {
        loop {
            // TODO(t/1395): This does not correctly handle
            // maintenance requests.
            sleep(Duration::from_millis(max_flush_wait_millis));
            let (result_tx, result_rx) = channel();
            let flush_req = WalRequestWrapper {
                request: WalRequest::FlushCallback,
                result_tx,
            };
            request_tx.send(flush_req).unwrap();
            let _ = result_rx.recv().unwrap();
        }
    }
}

// TODO(t/1348): Add WAL rotation to WalWriter.
pub(crate) struct WalWriter<WC: WriteCursor> {
    // TODO(t/1374): Support limiting concurrent writes.
    #[allow(dead_code)]
    max_concurrent_writes: Option<usize>,
    multi_threaded_writer: MultiWalWriter<WC>,
}

impl<WC: WriteCursor> Clone for WalWriter<WC> {
    fn clone(&self) -> Self {
        WalWriter {
            max_concurrent_writes: self.max_concurrent_writes,
            multi_threaded_writer: self.multi_threaded_writer.clone(),
        }
    }
}

impl<WC: WriteCursor> Debug for WalWriter<WC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalWriter")
            .field("max_concurrent_writes", &self.max_concurrent_writes)
            .finish()
    }
}

impl<WC: WriteCursor + Send + 'static> WalWriter<WC> {
    pub(crate) fn new(
        blob_writer: WC,
        max_concurrent_writes: Option<usize>,
        max_wal_size_bytes: usize,
        return_ok_before_flush: bool,
        max_flush_wait_millis: Option<u64>,
    ) -> Self {
        WalWriter {
            max_concurrent_writes,
            multi_threaded_writer: MultiWalWriter::new(
                blob_writer,
                max_concurrent_writes,
                return_ok_before_flush,
                max_flush_wait_millis,
                max_wal_size_bytes,
            ),
        }
    }

    pub(crate) fn write(&self, entry: WalEntry) -> Result<(), WalError> {
        self.multi_threaded_writer.write(entry)
    }

    pub(crate) fn close(self) -> Result<(), WalError> {
        self.multi_threaded_writer.close()
    }
}
