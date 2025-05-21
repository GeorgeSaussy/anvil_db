use std::sync::atomic::AtomicPtr;

pub(crate) struct MarkingPtr<T> {
    ptr: AtomicPtr<T>,
}

impl<T> From<Box<T>> for MarkingPtr<T> {
    fn from(boxed: Box<T>) -> Self {
        MarkingPtr {
            ptr: AtomicPtr::new(Box::into_raw(boxed)),
        }
    }
}

impl<T> std::fmt::Debug for MarkingPtr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let raw = self.ptr.load(std::sync::atomic::Ordering::SeqCst);
        let mark = MarkingPtr::get_mark_inner(raw);
        let raw = MarkingPtr::set_mark_raw(raw, false);
        write!(f, "MP ({mark} {raw:p})")
    }
}

impl<T> MarkingPtr<T> {
    pub(crate) fn null() -> Self {
        MarkingPtr {
            ptr: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    fn set_mark_raw(ptr: *mut T, mark_value: bool) -> *mut T {
        let z = 8 * std::mem::size_of::<usize>() - 1;
        let p = ptr as usize;
        let a = ((!0_usize) ^ (1 << z)) & p;
        let b = if mark_value { 1 } else { 0 };
        let b = b << z;
        let c: *const T = std::ptr::with_exposed_provenance(a + b);
        let d: *mut T = c.cast_mut();
        d
    }

    pub(crate) fn ptr_value(&self, order: std::sync::atomic::Ordering) -> *const T {
        let raw = self.ptr.load(order);

        MarkingPtr::set_mark_raw(raw, false)
    }

    fn get_mark_inner(ptr: *mut T) -> bool {
        let p = ptr as usize;
        let z = 8 * std::mem::size_of::<usize>() - 1;
        (p >> z) > 0
    }

    pub(crate) fn get_mark(&self, order: std::sync::atomic::Ordering) -> bool {
        let ptr = self.ptr.load(order);
        Self::get_mark_inner(ptr)
    }

    /// Sets the mark bit of the pointer to the given value.
    /// The mark bit is the most significant bit of the pointer.
    /// It returns the previous value of the mark bit.
    pub(crate) fn set_mark(&self, mark_value: bool, order: std::sync::atomic::Ordering) -> bool {
        let ptr = &self.ptr;
        loop {
            let current = ptr.load(order);
            let new = Self::set_mark_raw(current, mark_value);
            if let Ok(prev_raw) = ptr.compare_exchange(current, new, order, order) {
                return Self::get_mark_inner(prev_raw);
            }
            std::thread::yield_now();
        }
    }

    pub(crate) fn set_ptr_value(
        &self,
        new_value: *mut T,
        order: std::sync::atomic::Ordering,
    ) -> *mut T {
        let ptr = &self.ptr;
        loop {
            let current = ptr.load(order);
            let mark = Self::get_mark_inner(current);
            let marked_new_value = Self::set_mark_raw(new_value, mark);
            if let Ok(prev_raw) = ptr.compare_exchange(current, marked_new_value, order, order) {
                debug_assert_eq!(
                    Self::get_mark_inner(prev_raw),
                    mark,
                    "Mark bit should be the same as the current value"
                );
                return Self::set_mark_raw(prev_raw, false);
            }
            std::thread::yield_now();
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_mark_1() {
        let mp = MarkingPtr::null();
        assert_eq!(
            mp.ptr_value(std::sync::atomic::Ordering::SeqCst),
            std::ptr::null_mut()
        );
        let mut x = 0;
        let raw_x = &mut x as *mut _;
        mp.set_ptr_value(&mut x as *mut _, std::sync::atomic::Ordering::SeqCst);
        assert!(!mp.get_mark(std::sync::atomic::Ordering::SeqCst));
        assert!(!mp.get_mark(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(mp.ptr_value(std::sync::atomic::Ordering::SeqCst), raw_x);
        mp.set_mark(true, std::sync::atomic::Ordering::SeqCst);

        assert!(mp.get_mark(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(mp.ptr_value(std::sync::atomic::Ordering::SeqCst), raw_x);
        mp.set_mark(false, std::sync::atomic::Ordering::SeqCst);
        assert!(!mp.get_mark(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(mp.ptr_value(std::sync::atomic::Ordering::SeqCst), raw_x);
    }

    #[test]
    fn test_contention() {
        let order = std::sync::atomic::Ordering::SeqCst;
        let mut x = 0_usize;
        let raw_x = &mut x as *mut _;
        let mp = MarkingPtr::null();
        mp.set_ptr_value(raw_x, order);
        let mp_2 = Arc::new(mp);
        std::thread::scope(move |s| {
            for _ in 0..1000 {
                let mp_3 = mp_2.clone();
                s.spawn(move || {
                    let mp = &*mp_3;
                    for _ in 0..10_000 {
                        while mp.set_mark(true, order) {
                            std::thread::yield_now();
                        }
                        assert!(mp.get_mark(order));
                        assert!(mp.set_mark(false, order));
                    }
                });
            }
        });
    }
}
