use std::sync::atomic::Ordering;

/// TODO(1577): Can this be relaxed anywhere?
pub(crate) const DEFAULT_ORDERING: Ordering = Ordering::SeqCst;
