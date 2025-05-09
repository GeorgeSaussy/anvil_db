use std::sync::mpsc::{Receiver, Sender};

use crate::helpful_macros::raw_spawn;

type Task = Box<dyn FnOnce() + Send + 'static>;
enum Assignment {
    Task(Task),
    /// This will be used in testing to signal to the threads that they will
    /// not be used.
    Unused,
}

/// The [BackgroundExecutor] does not help with performance. Instead it is
/// a way to keep track of all background threads. This will make it easier to
/// maintain these threads and to ensure that they are all stopped when the
/// process is shutting down.
#[derive(Clone, Debug)]
pub(crate) struct BackgroundExecutor {
    wal_tx: Sender<Assignment>,
    compactor_tx: Sender<Assignment>,
}

impl BackgroundExecutor {
    fn task_tx() -> Sender<Assignment> {
        let (task_tx, task_rx) = std::sync::mpsc::channel();
        raw_spawn!(move || {
            let task_rx: Receiver<Assignment> = task_rx;
            while let Ok(assignment) = task_rx.recv() {
                match assignment {
                    Assignment::Task(task) => task(),
                    Assignment::Unused => {}
                }
            }
        });
        task_tx
    }

    pub(crate) fn new() -> Self {
        let wal_tx = Self::task_tx();
        let compactor_tx = Self::task_tx();
        BackgroundExecutor {
            wal_tx,
            compactor_tx,
        }
    }

    pub(crate) fn spawn_compactor_bg<F: FnOnce() + Send + 'static>(&self, compactor_bg: F) {
        let assignment = Assignment::Task(Box::new(compactor_bg));
        self.compactor_tx.send(assignment).unwrap();
    }
}

impl Drop for BackgroundExecutor {
    fn drop(&mut self) {
        self.wal_tx.send(Assignment::Unused).unwrap();
        self.compactor_tx.send(Assignment::Unused).unwrap();
    }
}
