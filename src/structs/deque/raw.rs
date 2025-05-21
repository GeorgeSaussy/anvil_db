use super::marks::get_mark;

#[allow(clippy::mut_from_ref)]
pub(crate) fn unsafe_mut_ref<T>(ptr: &*mut T) -> &mut T {
    debug_assert!(!ptr.is_null());
    debug_assert!(!get_mark(*ptr));
    unsafe { &mut **ptr }
}
