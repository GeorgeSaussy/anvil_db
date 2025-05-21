use std::marker::PhantomData;
use std::sync::atomic::{AtomicU32, Ordering};

use super::bac_interface::BacInterface;
use super::cleanup::{CleanupPolicy, DefaultCleanupPolicy};
use super::memory_interface::MemoryInterface;
use super::node::Node;

/// [BewareAndCleanup] wraps the global state from the paper
/// "Efficient and Reliable Lock-Free Memory Reclamation Based on
/// Reference Counting" by A. Gidenstam, et. al.
/// TODO(t/1571): This needs custom drop logic to clean-up all memory.
pub(crate) struct BewareAndCleanup<C, P> {
    // TODO(t/1594): This should probably be stack allocated. Either way,
    // the `Drop` needs to be implemented to clean up the memory.
    inner: *mut BewareAndCleanupInner<C, P>,
}

/// The paper includes a 'nr_links_nodes' to store the maximum number of
/// nodes that a node can reference. However, this value is never used
/// in this implementation because the vectors that store these lengths
/// cannot be statically sized. See [Node::links] for more information.
pub(crate) struct BewareAndCleanupInner<C, P> {
    /// Maximum number of threads that will be using the memory.
    /// This is a global variable in the paper.
    nr_threads: usize,

    /// maximum number of hazard pointers per thread
    /// This is a global variable in the paper.
    // TODO(t/1594): This has to be able to grow.
    nr_indices: usize,
    /// This is a global variable in the paper.
    threshold_c: usize,
    /// This is a global variable in the paper.
    threshold_s: usize,

    /// hp[thread_id][index] is a pointer to a node.
    /// hp.len() is self.nr_threads and
    /// hp[thread_id].len() is self.nr_indices.
    ///
    /// TODO(t/1571): Convert this to a linked list to remove the limit on the
    /// number of threads. See [Michael 2004][1] for more information.
    ///
    /// [1]: https://www.cs.otago.ac.nz/cosc440/readings/hazard-pointers.pdf
    hazard_pointers: Vec<Vec<*mut Node<P>>>,

    /// dl_nodes[thread_id][index] is a pointer to a node.
    /// dl_nodes.len() is self.nr_threads.
    /// dl_nodes[thread_id].len() is self.threshold_c.
    dl_nodes: Vec<Vec<*mut Node<P>>>,

    /// dl_claims.len() is self.nr_threads.
    /// dl_claims[thread_id].len() is self.threshold_c.
    dl_claims: Vec<Vec<AtomicU32>>,

    /// dl_done.len() is self.nr_threads.
    /// dl_done[thread_id].len() is self.threshold_c.
    dl_done: Vec<Vec<bool>>,

    _cleanup_policy: PhantomData<C>,
}

unsafe impl<C, UserProps> Send for BewareAndCleanup<C, UserProps> {}

impl<C, UserProps> BewareAndCleanup<C, UserProps> {
    pub(crate) fn new(nr_threads: usize, nr_indices: usize, threshold_c: usize) -> Self {
        // TODO(t/1571): Is this a reasonable default for threshold_s?
        // Should it be exposed as a parameter?
        let threshold_s = threshold_c * 2;

        let hazard_pointers: Vec<Vec<_>> = (0..nr_threads)
            .map(|_| (0..nr_indices).map(|_| std::ptr::null_mut()).collect())
            .collect();
        let dl_nodes: Vec<Vec<_>> = (0..nr_threads)
            .map(|_| (0..threshold_c).map(|_| std::ptr::null_mut()).collect())
            .collect();
        let dl_claims: Vec<Vec<_>> = (0..nr_threads)
            .map(|_| (0..threshold_c).map(|_| AtomicU32::new(0)).collect())
            .collect();
        let dl_done: Vec<Vec<_>> = (0..nr_threads)
            .map(|_| (0..threshold_c).map(|_| false).collect())
            .collect();
        let inner = Box::new(BewareAndCleanupInner {
            nr_threads,
            nr_indices,
            threshold_c,
            threshold_s,
            hazard_pointers,
            dl_nodes,
            dl_claims,
            dl_done,
            _cleanup_policy: PhantomData,
        });
        let inner = Box::<BewareAndCleanupInner<_, _>>::into_raw(inner);
        BewareAndCleanup { inner }
    }
}

impl<UserProps, C: CleanupPolicy<UserProps = UserProps>> BewareAndCleanup<C, UserProps> {
    pub(crate) fn view(&self, thread_id: usize) -> BacInterface<C, UserProps> {
        // TODO(t/1571): thread_id should be assigned automatically.
        let (threshold_c, threshold_s) = {
            let inner = unsafe { &*self.inner };
            (inner.threshold_c, inner.threshold_s)
        };

        BacInterface::<C, UserProps>::new(thread_id, threshold_c, threshold_s, self.inner)
    }
}

impl<UserProps, C: CleanupPolicy<UserProps = UserProps>> BewareAndCleanupInner<C, UserProps> {
    // TODO(t/1571): Reorganize the getter methods.
    pub(crate) fn hazard_pointers(&mut self, thread_id: usize) -> Vec<&mut *mut Node<UserProps>> {
        self.hazard_pointers[thread_id].iter_mut().collect()
    }

    pub(crate) fn hazard_pointers_ref(&self) -> &Vec<Vec<*mut Node<UserProps>>> {
        &self.hazard_pointers
    }

    pub(crate) fn deletion_list_nodes(
        &mut self,
        thread_id: usize,
    ) -> &mut Vec<*mut Node<UserProps>> {
        &mut self.dl_nodes[thread_id]
    }

    pub(crate) fn dl_claims(&mut self, thread_id: usize) -> &mut Vec<AtomicU32> {
        &mut self.dl_claims[thread_id]
    }

    pub(crate) fn dl_done(&mut self, thread_id: usize) -> &mut Vec<bool> {
        &mut self.dl_done[thread_id]
    }

    /// This is based on the `CleanUpAll` procedure in the paper in
    /// the 'Program 5' listing on page 6.
    pub(crate) fn clean_up_all<Mem: MemoryInterface<NodeProps = UserProps>>(
        &self,
        memory: &mut Mem,
    ) {
        // > 01 | for thread := 0 to NR_THREADS-1 do
        // > 02 |     for index := 0 to THRESHOLD_C-1 do
        // > 03 |         node := DL_Nodes[thread][index];
        // > 04 |         if node != NULL and not DL_Done[thread][index] then
        // > 05 |             FAA(&DL_Claims[thread][index],1);
        // > 06 |             if node = DL_Nodes[thread][index] then
        // > 07 |                 CleanUpNode(node);
        // > 08 |             FAA(&DL_Claims[thread][index],-1);
        for thread_id in 0..self.nr_threads {
            for index in 0..self.threshold_c {
                let node = self.dl_nodes[thread_id][index];
                if node.is_null() || self.dl_done[thread_id][index] {
                    continue;
                }
                self.dl_claims[thread_id][index].fetch_add(1, Ordering::SeqCst);
                if std::ptr::eq(node, self.dl_nodes[thread_id][index]) {
                    C::clean_up_node(memory, node);
                }
                self.dl_claims[thread_id][index].fetch_sub(1, Ordering::SeqCst);
            }
        }
    }

    pub(crate) fn clean_up_node<Mem: MemoryInterface<NodeProps = UserProps>>(
        memory: &mut Mem,
        node: *mut Node<UserProps>,
    ) {
        C::clean_up_node(memory, node)
    }
}

pub(crate) type SimpleBewareAndCleanup<P> = BewareAndCleanup<DefaultCleanupPolicy<P>, P>;
