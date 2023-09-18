#[cfg(test)]
use std::fs::{create_dir, remove_dir_all};

#[cfg(test)]
pub(crate) fn set_up(s: &str) -> String {
    use crate::logging::debug;

    let path = format!("/tmp/.jupiter_db_{}_test_data", s);
    if remove_dir_all(&path).is_ok() {
        debug!("Cleaned up old test directory.");
    }
    match create_dir(&path) {
        Ok(_) => {}
        Err(_) => {
            panic!("Could not create test directory!");
        }
    }
    path
}

#[cfg(test)]
pub(crate) fn tear_down(work_dir: &str) {
    match remove_dir_all(work_dir) {
        Ok(_) => {}
        Err(_) => {
            panic!("Cleanup failed!");
        }
    };
}
