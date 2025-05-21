use std::sync::atomic::{AtomicPtr, Ordering};

pub(crate) fn set_mark<T>(ptr: &AtomicPtr<T>, mark_value: bool) {
    loop {
        let current = ptr.load(Ordering::SeqCst);
        let new = set_mark_raw(current, mark_value);
        if ptr
            .compare_exchange(current, new, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return;
        }
    }
}

pub(crate) fn set_mark_raw<T>(ptr: *mut T, mark_value: bool) -> *mut T {
    let z = 8 * std::mem::size_of::<usize>() - 1;
    let p = ptr as usize;
    let a = ((!0_usize) ^ (1 << z)) & p;
    let b = if mark_value { 1 } else { 0 };
    let b = b << z;
    let c: *const T = std::ptr::with_exposed_provenance(a + b);
    let d: *mut T = c.cast_mut();
    d
}

pub(crate) fn get_mark<T>(ptr: *mut T) -> bool {
    let p = ptr as usize;
    let z = 8 * std::mem::size_of::<usize>() - 1;

    (p >> z) > 0
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_basic_get_mark() {
        let z = (8 * std::mem::size_of::<usize>()) - 1;
        let p = 1 << z;
        let c: *const usize = std::ptr::with_exposed_provenance(p);
        let d: *mut usize = c.cast_mut();
        assert!(get_mark(d));
    }

    #[test]
    fn test_ptr_marking() {
        let top = 10000;
        let mut fail_count = 0;
        for _ in 0..top {
            let ptr = Box::new(42);
            let raw = Box::into_raw(ptr);
            if get_mark(raw) {
                fail_count += 1;
            }
        }
        assert_eq!(fail_count, 0);
        let ptr = Box::new(42);
        let raw = Box::into_raw(ptr);
        assert!(!get_mark(raw));

        let raw = set_mark_raw(raw, true);
        assert!(get_mark(raw));
    }
}
