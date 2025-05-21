use std::sync::atomic::{AtomicPtr, AtomicU32};

pub(crate) struct Node<T> {
    mm_ref: AtomicU32,
    mm_trace: bool,
    mm_del: bool,
    user_props: T,
}

impl<T> Node<T> {
    pub(crate) fn new(user_props: T) -> Self {
        Self {
            mm_ref: AtomicU32::new(0),
            mm_trace: false,
            mm_del: false,
            user_props,
        }
    }

    pub(crate) fn value_ref(&self) -> &T {
        &self.user_props
    }

    pub(crate) fn value_mut_ref(&mut self) -> &mut T {
        &mut self.user_props
    }

    pub(crate) fn mm_ref_ref(&self) -> &AtomicU32 {
        &self.mm_ref
    }

    pub(crate) fn mm_trace_ref(&self) -> &bool {
        &self.mm_trace
    }

    pub(crate) fn mm_trace_ref_mut(&mut self) -> &mut bool {
        &mut self.mm_trace
    }

    pub(crate) fn mm_del_ref(&self) -> &bool {
        &self.mm_del
    }

    pub(crate) fn mm_del_ref_mut(&mut self) -> &mut bool {
        &mut self.mm_del
    }
}

impl<T: UserProps> Node<T> {
    pub(crate) fn links(&self) -> Vec<&AtomicPtr<Node<T>>> {
        self.user_props.links()
    }
}

pub(crate) trait UserProps: Sized {
    fn links(&self) -> Vec<&AtomicPtr<Node<Self>>>;
}

pub(crate) struct Link<NP, LP> {
    ptr: AtomicPtr<Node<NP>>,
    mark: LP,
}
