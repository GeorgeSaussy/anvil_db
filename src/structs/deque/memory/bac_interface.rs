use std::sync::atomic::{AtomicPtr, Ordering};

use super::bac_shared::BewareAndCleanupInner;
use super::cleanup::CleanupPolicy;
use super::memory_interface::MemoryInterface;
use super::node::Node;
use crate::structs::deque::{
    common::DEFAULT_ORDERING,
    marks::{get_mark, set_mark_raw},
};

/// The API described in [Gidenstam et. al. 2009][1] assumes that the caller
/// passes their thread_id for certain function calls and that the no concurrent
/// operations can use the same thread_id. However, instead of leaving it to
/// the caller to ensure that the thread_id is unique, can force requests to go
/// through a struct that cannot be safely shared between threads. Note that
/// [BewareAndCleanup] implements
/// [Send], so we share `Arc<Mutex<BewareAndCleanup>>` across threads and
/// construct a new [BacInterface] with [BewareAndCleanup::view]. A thread_id
/// is automatically assigned for the new [BacInterface] instance and it will
/// be used for all subsequent requests.
///
/// [1]: https://doi.ieeecomputersociety.org/10.1109/ISPAN.2005.42
/// [BewareAndCleanup]: crate::memory::bac_shared::BewareAndCleanup
/// [BewareAndCleanup::view]: crate::memory::bac_shared::BewareAndCleanup::view
/// [BacInterface]: crate::memory::bac_interface::BacInterface
pub(crate) struct BacInterface<C, NodeProps> {
    /// This is the maximum number of dirty references awaiting local
    /// cleanup that a thread can have. The current number of waiting references
    /// is stored in [BacInterface::num_dirty_refs]. This is enforced by checks
    /// in [BacInterface::delete_node].
    ///
    /// In [Gidenstam et. al. 2009][1], this value is described in Program 2 on
    /// page 5. There, the value is stored in the global
    /// constant `THRESHOLD_C`. Here, it is included in the interface struct
    /// to avoid unsafe memory reads. Instead of being global values, the
    /// shared variables are members of [BewareAndCleanupInner] which
    /// can be stack allocated. (In this implementation [BewareAndCleanupInner]
    /// is heap allocated, but it can be reworked for stack allocation without
    /// affecting the rest of the implementation.)
    ///
    /// Note: The interface struct should not outlive its
    /// [BewareAndCleanupInner] instance and we should be able to have an unsafe
    /// read for [BacInterface::max_dirty_refs_before_local_cleanup]. However,
    /// because safety not enforced by the compiler, we use slightly more
    /// memory per thread so that unrelated code changes do not cause breaking
    /// errors down the line.
    ///
    /// [1]: https://doi.ieeecomputersociety.org/10.1109/ISPAN.2005.42
    max_dirty_refs_before_local_cleanup: usize,

    /// If a thread has more than [BacInterface::max_dirty_refs_before_scan]
    /// dirty references when [BacInterface::delete_node] is called,
    /// [BacInterface::scan] is called to clean up all nodes that do not
    /// have references in other threads or nodes.
    ///
    /// In [Gidenstam et. al. 2009][1],this value is described in Program 2 on
    /// page 5. There, the value is stored in the global
    /// constant `THRESHOLD_S`. See the note in the documentation for
    /// [BacInterface::max_dirty_refs_before_local_cleanup] for more
    /// information about why it is a local value.
    ///
    /// [1]: https://doi.ieeecomputersociety.org/10.1109/ISPAN.2005.42
    max_dirty_refs_before_scan: usize,
    /// The current number of dirty references awaiting local cleanup.
    /// See [BacInterface::max_dirty_refs_before_local_cleanup]
    /// for more information.
    num_dirty_refs: usize,
    /// the thread that owns this interface instance
    thread_id: usize,

    /// An index corresponding to the next node that needs to be cleaned up.
    /// In [Gidenstam et. al. 2009][1], this value is called `dlist` and it
    /// is described in Program 2 on page 5.
    ///
    /// [1]: https://doi.ieeecomputersociety.org/10.1109/ISPAN.2005.42
    deletion_list_idx: Option<usize>,

    /// a pointer to the shared state for the garbage collector
    bac: *mut BewareAndCleanupInner<C, NodeProps>,

    /// The next deletion list is used to figure out which node the thread
    /// should clean up next. The element stored at index `i` in
    /// [BacInterface::next_deletion_list] is the index of next node to clean
    /// up in [BacInterface::deletion_list_nodes].
    /// The vector is always initialized to have a length of
    /// [BacInterface::max_dirty_refs_before_local_cleanup], which does not
    /// change for the lifetime of the struct.
    /// In [Gidenstam et. al. 2009][1], this value is called `DL_Nexts` and it
    /// is described in Program 2 on page 5.
    ///
    /// [1]: https://doi.ieeecomputersociety.org/10.1109/ISPAN.2005.42
    next_deletion_list: Vec<Option<usize>>,
}

impl<C, UserProps> BacInterface<C, UserProps> {
    pub(crate) fn new(
        thread_id: usize,
        threshold_c: usize,
        threshold_s: usize,
        bac: *mut BewareAndCleanupInner<C, UserProps>,
    ) -> Self {
        Self {
            max_dirty_refs_before_local_cleanup: threshold_c,
            max_dirty_refs_before_scan: threshold_s,
            thread_id,
            deletion_list_idx: None,
            num_dirty_refs: 0,
            bac,
            next_deletion_list: vec![None; threshold_c],
        }
    }
}

impl<UserProps, C: CleanupPolicy<UserProps = UserProps>> BacInterface<C, UserProps> {
    #[allow(clippy::mut_from_ref)]
    fn hazard_pointers(&self) -> Vec<&mut *mut Node<UserProps>> {
        let bac: &mut BewareAndCleanupInner<C, UserProps> = unsafe { &mut *self.bac };
        bac.hazard_pointers(self.thread_id)
    }

    fn deletion_list_nodes(&mut self) -> &mut Vec<*mut Node<UserProps>> {
        let bac = unsafe { &mut *self.bac };
        bac.deletion_list_nodes(self.thread_id)
    }

    fn dl_done(&mut self) -> &mut Vec<bool> {
        let bac = unsafe { &mut *self.bac };
        bac.dl_done(self.thread_id)
    }

    /// This function cleans up the local deletion list.
    /// In [Gidenstam et. al. 2009][1], this is called `CleanUpLocal` and the
    /// original pseudocode is listed in Program 5 on page 6.
    ///
    /// [1]: https://doi.ieeecomputersociety.org/10.1109/ISPAN.2005.42
    ///
    /// ```text
    /// procedure CleanUpLocal()
    /// CL1     index := d_list;
    /// CL2     while index != ⊥ do
    /// CL3         node := DL Nodes[threadId][index];
    /// CL4         CleanUpNode(node);
    /// CL5         index := DL_Nexts[index];
    /// ```
    fn clean_up_local(&mut self) {
        // CL1 index := d_list;
        let mut index = self.deletion_list_idx;

        // CL2 while index != ⊥ do
        while let Some(index_val) = index {
            // CL3 node := DL Nodes[threadId][index];
            let node = self.deletion_list_nodes()[index_val];

            // CL4 CleanUpNode(node);
            BewareAndCleanupInner::<C, UserProps>::clean_up_node(self, node);

            // CL5 index := DL_Nexts[index];
            index = self.next_deletion_list[index_val];
        }
    }

    /// Scan reclaims all nodes that were deleted by the current thread and
    /// that are not referenced by any other node or hazard pointer.
    /// In [Gidenstam et. al. 2009][1], the pseudocode is listed in
    /// Program 5 on page 6.
    ///
    /// [1]: https://doi.ieeecomputersociety.org/10.1109/ISPAN.2005.42
    ///
    /// ```text
    /// procedure Scan()
    /// SC01    index := d_list;
    /// SC02    while index != ⊥ do
    /// SC03        node := DL_Nodes[threadId][index];
    /// SC04        if node.mm_ref = 0 then
    /// SC05            node.mm_trace := true;
    /// SC06            if node.mm_ref != 0 then
    /// SC07                node.mm_trace := false;
    /// SC08        index := DL_Nexts[index];
    /// SC09    plist := ∅; new_d_list := ⊥; new_d_count := 0;
    /// SC10    for thread := 0 to NR_THREADS-1 do
    /// SC11        for index := 0 to NR_INDICES-1 do
    /// SC12            node := HP[thread][index];
    /// SC13            if node != NULL then
    /// SC14                plist := plist + node;
    /// SC15    Sort and remove duplicates in array plist
    /// SC16    while d_list != ⊥ do
    /// SC17        index := d_list;
    /// SC18        node := DL_Nodes[threadId][index];
    /// SC19        d_list := DL_Nexts[index];
    /// SC20        if node.mm_ref = 0 and node.mm_trace and node not in plist then
    /// SC21            DL_Nodes[threadId][index] := NULL;
    /// SC22            if DL_Claims[threadId][index] = 0 then
    /// SC23                TerminateNode(node, false);
    /// SC24                Free the memory of node
    /// SC25                continue;
    /// SC26            TerminateNode(node, true);
    /// SC27            DL_Done[threadId][index] := true;
    /// SC28            DL_Nodes[threadId][index] := node;
    /// SC29        DL_Nexts[index] := new_d_list;
    /// SC30        new_d_list := index;
    /// SC31        new_d_count := new_d_count + 1;
    /// SC32    d_list := new_d_list;
    /// SC33    d_count := new_d_count;
    /// ```
    fn scan(&mut self) {
        // First, set Node::mm_trace as true for deleted nodes that have a
        // reference count of 0. The check at SC06 ensures that if the
        // reference count for the node were incremented between when the count
        // is checked at SC04 and the trace bit is set at SC05, the trace bit
        // is unset and the node will be skipped at SC20. The reference count
        // cannot be incremented once the trace bit is set, so the SC06 check
        // fails, it means the reference count was incremented between the
        // previous time is was checked at SC04 and when the bit was set at
        // SC05.

        /* line break */

        // SC01 index := d_list;
        let mut index_wrapper = self.deletion_list_idx;

        // SC02 while index != ⊥ do
        while let Some(index) = index_wrapper {
            // SC03 node := DL_Nodes[threadId][index];
            let dl_nodes = self.deletion_list_nodes();
            let node = dl_nodes[index];
            let node: &mut Node<_> = unsafe { &mut *node };

            // SC04 if node.mm_ref = 0 then
            if node.mm_ref_ref().load(DEFAULT_ORDERING) == 0 {
                // SC05 node.mm_trace := true;
                *node.mm_trace_ref_mut() = true;

                // SC06 if node.mm_ref != 0 then
                if node.mm_ref_ref().load(DEFAULT_ORDERING) != 0 {
                    // SC07 node.mm_trace := false;
                    *node.mm_trace_ref_mut() = false;
                }
            }

            // SC08 index := DL_Nexts[index];
            index_wrapper = self.next_deletion_list[index];
        }

        // Record all active hazard pointers in `p_list`.

        // SC09 plist := ∅; new_d_list := ⊥; new_d_count := 0;
        let mut p_list = Vec::new();
        let mut new_d_list = None;
        let mut new_d_count = 0;

        let bac = unsafe { &mut *self.bac };

        // SC10 for thread := 0 to NR_THREADS-1 do
        for nodes in bac.hazard_pointers_ref().iter() {
            // SC11 for index := 0 to NR_INDICES-1 do
            // SC12     node := HP[thread][index];
            for node in nodes {
                // SC13 if node != NULL then
                if !node.is_null() {
                    // SC14 plist := plist + node;
                    p_list.push(*node);
                }
            }
        }

        // SC15 Sort and remove duplicates in array p_list
        p_list.sort();
        p_list.dedup();

        // SC16 while d_list != ⊥ do
        // SC17 index := d_list;
        while let Some(index) = self.deletion_list_idx {
            // SC18     node := DL_Nodes[threadId][index];
            let node_ptr = bac.deletion_list_nodes(self.thread_id)[index];
            let node: &mut Node<_> = unsafe { &mut *node_ptr };

            // SC19 d_list := DL_Nexts[index];
            self.deletion_list_idx = self.next_deletion_list[index];

            // SC20 if node.mm_ref = 0 and node.mm_trace and node not in plist then
            if node.mm_ref_ref().load(DEFAULT_ORDERING) == 0
                && *node.mm_trace_ref()
                && !p_list.contains(&node_ptr)
            {
                // SC21 DL_Nodes[threadId][index] := NULL;
                bac.deletion_list_nodes(self.thread_id)[index] = std::ptr::null_mut();

                // SC22 if DL_Claims[threadId][index] = 0 then
                if bac.dl_claims(self.thread_id)[index].load(Ordering::SeqCst) == 0 {
                    // SC23 TerminateNode(node, false);
                    C::terminate_node(self, node, false);

                    // SC24 Free the memory of node
                    let _droppable_box = unsafe { Box::from_raw(node) };

                    // SC25 continue;
                    continue;
                }

                // SC26 TerminateNode(node, true);
                C::terminate_node(self, node, true);

                // SC27 DL_Done[threadId][index] := true;
                bac.dl_done(self.thread_id)[index] = true;

                // SC28 DL_Nodes[threadId][index] := node;
                bac.deletion_list_nodes(self.thread_id)[index] = node;
            }

            // SC29 DL_Nexts[index] := new_d_list;
            self.next_deletion_list[index] = new_d_list;

            // SC30 new_d_list := index;
            new_d_list = Some(index);

            // SC31 new_d_count := new_d_count + 1;
            new_d_count += 1;
        }

        // SC32 d_list := new_d_list;
        self.deletion_list_idx = new_d_list;

        // SC33 d_count := new_d_count;
        self.num_dirty_refs = new_d_count;
    }
}

impl<UserProps, C: CleanupPolicy<UserProps = UserProps>> MemoryInterface
    for BacInterface<C, UserProps>
{
    type NodeProps = UserProps;

    /// See the function 'CompareAndSwapRef' in the Program 3 listing on page 5
    /// of the paper.
    fn cas_ref(
        &mut self,
        link: &AtomicPtr<Node<UserProps>>,
        old: *mut Node<UserProps>,
        new: *mut Node<UserProps>,
    ) -> bool {
        if link
            .compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            // Specifically for use in the doubly linked list,
            // we need to ignore the mark that is store in the highest value
            // bit in some pointers.
            let new = set_mark_raw(new, false);
            let old = set_mark_raw(old, false);

            if !new.is_null() {
                let new = unsafe { &mut *new };
                new.mm_ref_ref().fetch_add(1, Ordering::SeqCst);
                *new.mm_trace_ref_mut() = false;
            }
            if !old.is_null() {
                let old = unsafe { &*old };
                old.mm_ref_ref().fetch_sub(1, Ordering::SeqCst);
            }
            return true;
        }
        false
    }

    /// See the function 'DerefLink' in the Program 3 listing on page 5
    /// of the paper.
    /// This function will panic if it cannot dereference the link.
    fn deref_link(&mut self, link: &AtomicPtr<Node<UserProps>>) -> *mut Node<UserProps> {
        // TODO(t/1594): Can we safely dereference marked pointers?
        // debug_assert!(!get_mark(link.load(Ordering::SeqCst)));

        // > 01 | Choose index such that HP[threadId][index] = NULL
        let mut hp = self.hazard_pointers();
        let option = hp.iter_mut().find(|ptr| (**ptr).is_null());
        let hazard_pointer = option.unwrap();

        // > 02 | while true do
        // > 03 |     node := *link;
        // > 04 |     HP[threadId][index] := node;
        // > 05 |     if *link = node then
        // > 06 |         return node;
        loop {
            // This is not an atomic operation in the paper.
            let node = link.load(DEFAULT_ORDERING);

            **hazard_pointer = node;

            if std::ptr::eq(link.load(DEFAULT_ORDERING), node) {
                return node;
            }
        }
    }

    /// See the procedure 'ReleaseRef' in the Program 3 listing on page 5
    /// of the paper.
    fn release_ref(&mut self, node: *mut Node<UserProps>) {
        debug_assert!(!get_mark(node));

        // > 01 | Choose index such that HP[threadId][index]=node
        let mut hp = self.hazard_pointers();
        let hazard_ptr = hp
            .iter_mut()
            .find(|ptr| std::ptr::eq(set_mark_raw(***ptr, false), node))
            .unwrap();

        // > 02 | HP[threadId][index] := NULL
        **hazard_ptr = std::ptr::null_mut();
    }

    /// See the procedure 'StoreRef' in the Program 3 listing on page 5
    /// of the paper.
    fn store_ref(&mut self, link: &AtomicPtr<Node<UserProps>>, node: *mut Node<UserProps>) {
        // TODO(t/1594): Are these assertions correct?
        debug_assert!(!get_mark(link.load(Ordering::SeqCst)));
        debug_assert!(!get_mark(node));

        // TODO(t/1577): Can this ordering be relaxed?
        // This operation is not atomic in the paper.
        // > S1 | old := *link;
        let old = link.load(DEFAULT_ORDERING);

        // TODO(t/1577): Can this ordering can be relaxed?
        // This operation is not atomic in the paper.
        // > S2 | *link := node;
        link.store(node, DEFAULT_ORDERING);

        // > S3 | if node != NULL then
        // > S4 |     FAA(&node.mm ref, 1);
        // > S5 |     node.mm_trace := false;
        if !node.is_null() {
            unsafe {
                (*node).mm_ref_ref().fetch_add(1, Ordering::SeqCst);
                *(*node).mm_trace_ref_mut() = false;
            }
        }

        // > S6 | if old != NULL then FAA(&old.mm_ref, -1);
        if !old.is_null() {
            unsafe {
                (*old).mm_ref_ref().fetch_sub(1, Ordering::SeqCst);
            }
        }
    }

    /// See the function 'NewNode' in the Program 3 listing on page 5
    /// of the paper.
    fn new_node(&mut self, user_props: UserProps) -> Option<*mut Node<UserProps>> {
        // > NN1 | node := Allocate the memory of node (e.g., using malloc)
        // > NN2 | node.mm ref := 0; node.mm trace := false; node.mm del := false;
        let node = Box::new(Node::new(user_props));

        // > NN3 | Choose index such that HP[threadId][index]=NULL
        let mut hp = self.hazard_pointers();
        let mut hp_iter = hp.iter_mut();
        let option = hp_iter.find(|ptr| (**ptr).is_null());
        let node_ptr = option?;

        // > NN4 | HP[threadId][index] := node;
        **node_ptr = Box::<Node<_>>::into_raw(node);

        // > NN5 | return node;
        Some(**node_ptr)
    }

    /// See the 'DeleteNode' procedure in the Program 3 listing on page 5
    /// of the paper.
    fn delete_node(&mut self, node: *mut Node<UserProps>) {
        // > DN01 | ReleaseRef(node);
        self.release_ref(node);

        // > DN02 | node.mm del := true; node.mm trace := false;
        unsafe {
            *(*node).mm_del_ref_mut() = true;
            *(*node).mm_trace_ref_mut() = false;
        }

        // > DN03 | Choose index such that DL_Nodes[threadId][index]=NULL
        let index = self
            .deletion_list_nodes()
            .iter_mut()
            .position(|ptr| (*ptr).is_null())
            .unwrap();

        // > DN04 | DL_Done[threadId][index]:=false;
        self.dl_done()[index] = false;

        // > DN05 | DL_Nodes[threadId][index]:=node;
        self.deletion_list_nodes()[index] = node;

        // > DN06 | DL_Nexts[index] := d_list;
        self.next_deletion_list[index] = self.deletion_list_idx;

        // > DN07 | d_list := index; d_count := d_count + 1;
        self.deletion_list_idx = Some(index);
        self.num_dirty_refs += 1;

        // > DN08 | while true do
        // > DN09 |     if d_count = THRESHOLD_C then CleanUpLocal();
        // > DN10 |     if d_count ≥ THRESHOLD_S then Scan();
        // > DN11 |     if d_count = THRESHOLD_C then CleanUpAll();
        // > DN12 |         else break;
        loop {
            if self.num_dirty_refs == self.max_dirty_refs_before_local_cleanup {
                self.clean_up_local();
            }

            if self.num_dirty_refs >= self.max_dirty_refs_before_scan {
                self.scan();
            }

            if self.num_dirty_refs == self.max_dirty_refs_before_local_cleanup {
                let bac = unsafe { &*self.bac };
                bac.clean_up_all(self);
            } else {
                break;
            }
        }
    }
}
