//! The purpose of the test-only [unwrap!], [clone!] and [spawn!]
//! macros is to make it easier
//! to keep track of when unwraps and clones are happening outside of tests.
//! By using this is tests it then becomes possible to grep for their use
//! outside of tests. Eventually, `.unwrap()` should be removed for all non-test
//! code. (Technically there are times where is it fine, but as a lint check,
//! it takes the burden of trying to figure out if any given context is one of
//! those times. On the other hand, there will be cases where `.clone()` is
//! needed and called for. We should just try to minimize those cases.
//!
//! At of now, migrating test code to use these is a work in progress.
//!
//! The macro [unlock!] is just for convenience.

#[cfg(test)]
macro_rules! clone {
    ($stmt:expr) => {
        ($stmt).clone()
    };
}
#[cfg(test)]
pub(crate) use clone;

#[cfg(test)]
macro_rules! unwrap {
    ($stmt:expr) => {
        ($stmt).unwrap()
    };
}
#[cfg(test)]
pub(crate) use unwrap;

#[cfg(test)]
macro_rules! spawn {
    ($stmt:expr) => {
        std::thread::spawn($stmt)
    };
}
#[cfg(test)]
pub(crate) use spawn;

macro_rules! raw_spawn {
    ($stmt:expr) => {
        std::thread::spawn($stmt)
    };
}
pub(crate) use raw_spawn;

macro_rules! unlock {
    ($stmt:expr) => {
        ($stmt).unwrap()
    };
}
pub(crate) use unlock;
