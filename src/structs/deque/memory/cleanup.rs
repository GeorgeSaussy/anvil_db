use std::marker::PhantomData;
use std::sync::atomic::Ordering;

use super::memory_interface::MemoryInterface;
use super::node::{Node, UserProps};

// See the description of 'TerminateNode' and 'CleanUpNode' on pg. 7 of
// "Efficient and Reliable Lock-Free Memory Reclamation Based on
// Reference Counting" by A. Gidenstam, et. al.
pub(crate) trait CleanupPolicy {
    type UserProps;
    fn clean_up_node<Mem: MemoryInterface<NodeProps = Self::UserProps>>(
        memory: &mut Mem,
        node: *mut Node<Self::UserProps>,
    );
    fn terminate_node<Mem: MemoryInterface<NodeProps = Self::UserProps>>(
        memory: &mut Mem,
        node: *mut Node<Self::UserProps>,
        concurrent: bool,
    );
}

pub(crate) struct DefaultCleanupPolicy<T> {
    _phantom: PhantomData<T>,
}

impl<T> Default for DefaultCleanupPolicy<T> {
    fn default() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T: UserProps> CleanupPolicy for DefaultCleanupPolicy<T> {
    type UserProps = T;

    /// See the 'TerminateNode' procedure in the Program 4 listing on page 6
    /// of the paper.
    fn terminate_node<Mem: MemoryInterface<NodeProps = Self::UserProps>>(
        memory: &mut Mem,
        node: *mut Node<Self::UserProps>,
        concurrent: bool,
    ) {
        if !concurrent {
            let node = unsafe { &mut *node };
            let links = node.links();
            for link in links {
                memory.store_ref(link, std::ptr::null_mut());
            }
        } else {
            let node = unsafe { &mut *node };
            let links = node.links();
            for link in links {
                loop {
                    // TODO(t/1571): I believe this ordering can be relaxed.
                    // This operation is not atomic in the paper.
                    let node_1_ptr = link.load(Ordering::SeqCst);
                    if memory.cas_ref(link, node_1_ptr, std::ptr::null_mut()) {
                        break;
                    }
                }
            }
        }
    }

    /// See the 'CleanUpNode' procedure in the Program 4 listing on page 6
    /// of the paper.
    fn clean_up_node<Mem: MemoryInterface<NodeProps = Self::UserProps>>(
        memory: &mut Mem,
        node: *mut Node<Self::UserProps>,
    ) {
        let node = unsafe { &mut *node };
        let links = node.links();
        for link in links {
            loop {
                let node_1_ptr = memory.deref_link(link);
                if node_1_ptr.is_null() {
                    memory.release_ref(node_1_ptr);
                    break;
                }
                let node_1: &Node<T> = unsafe { &*node_1_ptr };
                if !node_1.mm_del_ref() {
                    memory.release_ref(node_1_ptr);
                    break;
                }
                let node_2 = memory.deref_link(link);
                memory.cas_ref(link, node_1_ptr, node_2);
                memory.release_ref(node_2);
                memory.release_ref(node_1_ptr);
            }
        }
    }
}
