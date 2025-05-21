// This implementation is based on the paper
// "Lock-free deques and doubly linked lists" by Håkan Sundell and
// Philippas Tsigas.
//
// https://www.cse.chalmers.se/~tsigas/papers/deques-dlls-jpdc.pdf

use std::marker::PhantomData;
use std::sync::atomic::{AtomicPtr, Ordering};

use super::marks::{get_mark, set_mark, set_mark_raw};
use super::memory::bac_shared::BewareAndCleanup;
use super::memory::cleanup::CleanupPolicy;
use super::memory::memory_interface::MemoryInterface;
use super::memory::node::Node;
use super::raw::unsafe_mut_ref;

struct ListProps<T> {
    prev: AtomicPtr<Node<ListProps<T>>>,
    next: AtomicPtr<Node<ListProps<T>>>,
    value: Option<T>,
}

struct ListCleanupPolicy<T> {
    _phantom: PhantomData<T>,
}

impl<T> CleanupPolicy for ListCleanupPolicy<T> {
    type UserProps = ListProps<T>;

    /// From figure 8 in the paper:
    ///
    /// ```text
    /// procedure CleanUpNode(node: pointer to Node)
    /// CU01    while true do
    /// CU02        prev := DeRefLink(&node.prev);
    /// CU03        if prev.prev.d = false then break;
    /// CU04        prev_2 := DeRefLink(&prev.prev);
    /// CU05        CASRef(&node.prev, (prev.p, true), prev_2.p, true));
    /// CU06        ReleaseRef(prev_2, prev)
    /// CU07    while true do
    /// CU08        next := DeRefLink(&node.next);
    /// CU09        if next.d = false then break;
    /// CU10        next_2 := DeRefLink(&next.next);
    /// CU11        CASRef(&node.next, (next.p, true), next_2.p, true));
    /// CU12        ReleaseRef(next_2, next)
    /// CU13    ReleaseRef(prev, next);
    /// ```
    fn clean_up_node<Mem: MemoryInterface<NodeProps = Self::UserProps>>(
        memory: &mut Mem,
        node_ptr: *mut Node<Self::UserProps>,
    ) {
        // `prev_ptr` needs to be accessed outside the following loop.
        let mut prev_ptr: *mut Node<Self::UserProps>;

        // CU01 while true do
        loop {
            // CU02 prev := DeRefLink(&node.prev);
            let node = unsafe_mut_ref(&node_ptr);
            prev_ptr = memory.deref_link(&node.value_ref().prev);

            // CU03 if prev.prev.d = false then break;
            let prev = unsafe_mut_ref(&prev_ptr);
            let prev_prev_ptr = prev.value_ref().prev.load(Ordering::SeqCst);
            if !get_mark(prev_prev_ptr) {
                break;
            }

            // CU04 prev_2 := DeRefLink(&prev.prev);
            let prev_2 = memory.deref_link(&prev.value_ref().prev);

            // CU05 CASRef(&node.prev, (prev.p, true), prev_2.p, true));
            memory.cas_ref(
                &node.value_ref().prev,
                set_mark_raw(prev_ptr, true),
                set_mark_raw(prev_2, true),
            );

            // CU06 ReleaseRef(prev_2, prev)
            memory.release_ref(prev_2);
            memory.release_ref(prev);
        }

        // `next_ptr` needs to be accessed outside the following loop.
        let mut next_ptr: *mut Node<Self::UserProps>;

        // CU07 while true do
        loop {
            // CU08 next := DeRefLink(&node.next);
            let node = unsafe_mut_ref(&node_ptr);
            next_ptr = memory.deref_link(&node.value_ref().next);

            // CU09 if next.d = false then break;
            if !get_mark(next_ptr) {
                break;
            }

            // CU10 next_2 := DeRefLink(&next.next);
            let next = unsafe_mut_ref(&next_ptr);
            let next_2_ptr = memory.deref_link(&next.value_ref().next);

            // CU11 CASRef(&node.next, (next.p, true), next_2.p, true));
            memory.cas_ref(
                &node.value_ref().next,
                set_mark_raw(next_ptr, true),
                set_mark_raw(next_2_ptr, true),
            );

            // CU12 ReleaseRef(next_2, next)
            memory.release_ref(next_2_ptr);
            memory.release_ref(next_ptr);
        }

        // CU13 ReleaseRef(prev, next);
        memory.release_ref(prev_ptr);
        memory.release_ref(next_ptr);
    }

    /// From Fig. 8 in the paper:
    ///
    /// ```text
    /// procedure TerminateNode(node: pointer to Node)
    /// TN1    StoreRef(&node.prev, NULL);
    /// TN2    StoreRef(&node.next, NULL);
    /// ```
    fn terminate_node<Mem: MemoryInterface<NodeProps = Self::UserProps>>(
        memory: &mut Mem,
        node_ptr: *mut Node<Self::UserProps>,
        _concurrent: bool,
    ) {
        let node = unsafe_mut_ref(&node_ptr);

        // TN1 StoreRef(&node.prev, NULL);
        memory.store_ref(&node.value_ref().prev, std::ptr::null_mut());
        // TN2 StoreRef(&node.next, NULL);
        memory.store_ref(&node.value_ref().next, std::ptr::null_mut());
    }
}

pub struct List<T: 'static> {
    bac: BewareAndCleanup<ListCleanupPolicy<T>, ListProps<T>>,
    head: AtomicPtr<Node<ListProps<T>>>,
    tail: AtomicPtr<Node<ListProps<T>>>,
    init_wait_nanos: u64,
}

impl<T: Clone> Default for List<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone> List<T> {
    /// Create a new, empty [List](struct@List) instance.
    pub fn new() -> Self {
        let bac = BewareAndCleanup::new(5, 1024, 1024);

        let head: Box<Node<ListProps<T>>> = Box::new(Node::new(ListProps {
            prev: AtomicPtr::new(std::ptr::null_mut()),
            next: AtomicPtr::new(std::ptr::null_mut()),
            value: None,
        }));
        let tail: Box<Node<ListProps<T>>> = Box::new(Node::new(ListProps {
            prev: AtomicPtr::new(std::ptr::null_mut()),
            next: AtomicPtr::new(std::ptr::null_mut()),
            value: None,
        }));

        let tail_ptr = Box::into_raw(tail);
        (*head).value_ref().next.store(tail_ptr, Ordering::SeqCst);

        let head_ptr = Box::into_raw(head);
        let tail = unsafe_mut_ref(&tail_ptr);
        (*tail).value_ref().prev.store(head_ptr, Ordering::SeqCst);

        let queue = Self {
            bac,
            head: AtomicPtr::new(head_ptr),
            tail: AtomicPtr::new(tail_ptr),
            init_wait_nanos: 5,
        };

        let mut bac_view = Box::new(queue.bac.view(0));
        bac_view.store_ref(&queue.head, head_ptr);
        bac_view.store_ref(&queue.tail, tail_ptr);

        queue
    }

    fn view(&self, thread_id: usize) -> ListInterface<'_, T> {
        let bac_view = Box::new(self.bac.view(thread_id));
        ListInterface {
            bac_view,
            head: &self.head,
            tail: &self.tail,
            init_wait_nanos: self.init_wait_nanos,
        }
    }
}

struct ListInterface<'a, T> {
    bac_view: Box<dyn MemoryInterface<NodeProps = ListProps<T>>>,
    head: &'a AtomicPtr<Node<ListProps<T>>>,
    tail: &'a AtomicPtr<Node<ListProps<T>>>,
    init_wait_nanos: u64,
}

unsafe impl<'a, T> Send for ListInterface<'a, T> {}

impl<'a, T: Clone> ListInterface<'a, T> {
    /// See the function 'CreateNode' in figure 4 on page 1010 of the paper.
    fn create_node(&mut self, value: T) -> *mut Node<ListProps<T>> {
        // > CN1 | node := NewNode();
        // > CN2 | node.value := value;
        let props = ListProps {
            prev: AtomicPtr::new(std::ptr::null_mut()),
            next: AtomicPtr::new(std::ptr::null_mut()),
            value: Some(value),
        };

        // > CN3 | return node;
        self.bac_view.new_node(props).unwrap()
    }

    fn back_off(&self, _wait_time: &mut u64) {
        std::thread::yield_now();
        // TODO(gs): Should backoff be implemented? e.g.
        //
        // > std::thread::sleep(Duration::from_nanos(*wait_time));
        // > *wait_time *= 2;
    }

    /// See the function 'CorrectPrev' in figure 15 on page 1016 of the paper.
    fn correct_prev(
        &mut self,
        prev_ptr: *mut Node<ListProps<T>>,
        node_ptr: *mut Node<ListProps<T>>,
    ) -> *mut Node<ListProps<T>> {
        let mut wait_nanos = self.init_wait_nanos;

        let mut prev_ptr = prev_ptr;

        // > CP01 | last_link := NULL;
        let mut last_link_ptr: *mut Node<ListProps<T>> = std::ptr::null_mut();

        // > CP02 | while true do
        loop {
            // Pseudocode within the CP02 loop is unindented by one level for
            // readability.
            //
            // > CP03 | link_1 := node.prev; if link_1.d = true then break;
            let node = unsafe_mut_ref(&node_ptr);
            let link_1_ptr = node.value_ref().prev.load(Ordering::SeqCst);
            if get_mark(link_1_ptr) {
                break;
            }

            // > CP04 | prev_2 := DeRefLink(&prev.next);
            let ptr = set_mark_raw(prev_ptr, false);
            let prev = unsafe_mut_ref(&ptr);
            let mut prev_2_ptr = self.bac_view.deref_link(&prev.value_ref().next);

            // > CP05 | if prev_2.d = true then
            // > CP06 |     if last_link != NULL then
            // > CP07 |         SetMark(&prev.prev);
            // > CP08 |         CASRef(&last_link.next, prev, (prev_2.p, false));
            // > CP09 |         ReleaseRef(prev_2.p, prev);
            // > CP10 |         prev := last_link; last_link = NULL;
            // > CP11 |         continue;
            // > CP12 |     ReleaseRef(prev_2.p);
            // > CP13 |     prev_2 := DeRefLink(&prev.prev);
            // > CP14 |     ReleaseRef(prev); prev := prev_2;
            // > CP15 |     continue;
            if get_mark(prev_2_ptr) {
                if !last_link_ptr.is_null() {
                    set_mark(&prev.value_ref().prev, true);
                    let ptr = set_mark_raw(last_link_ptr, false);
                    let last_link = unsafe_mut_ref(&ptr);
                    self.bac_view.cas_ref(
                        &last_link.value_ref().next,
                        prev_ptr,
                        set_mark_raw(prev_2_ptr, false),
                    );
                    self.bac_view.release_ref(set_mark_raw(prev_2_ptr, false));
                    self.bac_view.release_ref(prev_ptr);
                    prev_ptr = last_link_ptr;
                    last_link_ptr = std::ptr::null_mut();
                    continue;
                }
                self.bac_view.release_ref(set_mark_raw(prev_2_ptr, false));
                prev_2_ptr = self.bac_view.deref_link(&prev.value_ref().prev);

                // TODO(t/1571): Does this need to be unmarked?
                self.bac_view.release_ref(set_mark_raw(prev_ptr, false));
                prev_ptr = prev_2_ptr;

                continue;
            }

            // > CP16 | if prev_2 != node then
            // > CP17 |     if last_link != NULL then ReleaseRef(last_link);
            // > CP18 |     last_link := prev;
            // > CP19 |     prev := prev_2;
            // > CP20 |     continue;
            if !std::ptr::eq(prev_2_ptr, node_ptr) {
                // TODO(t/1571): Do we need to unmark the reference?
                let unmasked_last_link_ptr = set_mark_raw(last_link_ptr, false);
                if !unmasked_last_link_ptr.is_null() {
                    self.bac_view.release_ref(unmasked_last_link_ptr);
                }
                last_link_ptr = prev_ptr;
                prev_ptr = prev_2_ptr;
                continue;
            }

            // > CP21 | ReleaseRef(prev_2);
            self.bac_view.release_ref(prev_2_ptr);

            // > CP22 | if CASRef(&node.prev, link_1, (prev, false)) then
            // > CP23 |         if prev.prev.d = true then continue;
            // > CP24 |         break;
            // > CP25 |     Back-Off
            if self.bac_view.cas_ref(
                &node.value_ref().prev,
                link_1_ptr,
                set_mark_raw(prev_ptr, false),
            ) {
                if get_mark(prev.value_ref().prev.load(Ordering::SeqCst)) {
                    continue;
                }
                break;
            }

            self.back_off(&mut wait_nanos);
        }

        // > CP26 | if last_link != NULL then ReleaseRef(last_link);
        if !last_link_ptr.is_null() {
            // TODO(t/1571): Do we need to unmark the reference?
            self.bac_view
                .release_ref(set_mark_raw(last_link_ptr, false));
        }

        // > CP27 | return prev;
        prev_ptr
    }

    /// ```text
    /// procedure PushEnd(node, next: pointer to node)
    /// P1    while true do
    /// P2        link_1 := next.prev;
    /// P3        if link_1.d = true or node.next != <next, false> then break;
    /// P4        if CASRef(&next.prev, link_1, <node, false>)
    /// P5            if node.prev.d = true then
    /// P6                node := CorrectPrev(node, next);
    /// P7            break;
    /// P8        Back-Off
    /// P9    ReleaseRef(next, node);
    /// ```
    fn push_end(&mut self, node_ptr: *mut Node<ListProps<T>>, next_ptr: *mut Node<ListProps<T>>) {
        let mut wait_time = self.init_wait_nanos;
        let mut node_ptr = node_ptr;

        // P1 while true do
        loop {
            debug_assert!(!next_ptr.is_null());
            // P2 link_1 := next.prev;
            let next = unsafe_mut_ref(&next_ptr);
            let link_1_ptr = next.value_ref().prev.load(Ordering::SeqCst);

            // P3 if link_1.d = true or node.next != (next, false) then break;
            let node = unsafe_mut_ref(&node_ptr);
            let b_1 = get_mark(link_1_ptr);
            let node_dot_next_ptr: *mut _ = node.value_ref().next.load(Ordering::SeqCst);
            let b_2 = !std::ptr::eq(node_dot_next_ptr, set_mark_raw(next, false));
            if b_1 || b_2 {
                break;
            }

            // P4 if CASRef(&next.prev, link_1, (node, false))
            let new = set_mark_raw(node_ptr, false);
            if self
                .bac_view
                .cas_ref(&next.value_ref().prev, link_1_ptr, new)
            {
                // P5 if node.prev.d = true then
                let node_dot_prev_ptr: *mut _ = node.value_ref().prev.load(Ordering::SeqCst);
                if get_mark(node_dot_prev_ptr) {
                    // P6 node := CorrectPrev(node, next);
                    node_ptr = self.correct_prev(node_ptr, next_ptr);
                }

                // P7 break;
                break;
            }

            // P8 Back-Off
            self.back_off(&mut wait_time);
        }

        // P9 ReleaseRef(next, node);
        self.bac_view.release_ref(next_ptr);
        self.bac_view.release_ref(node_ptr);
    }

    /// ```text
    /// procedure PushLeft(value: pointer to word)
    /// L01    node := CreateNode(value);
    /// L02    prev := DeRefLink(&head);
    /// L03    next := DeRefLink(&prev.next);
    /// L04    while true do
    /// L05        StoreRef(&node.prev, <prev, false>));
    /// L06        StoreRef(&node.next, <next, false>));
    /// L07        if CASRef(&prev.next, <next, false>, <node, false>) then break;
    /// L08        ReleaseRef(next);
    /// L09        next := DeRefLink(&prev.next);
    /// L10        Back-Off
    /// L11    PushEnd(node, next);
    /// ```
    fn push_left(&mut self, value: T) {
        let mut wait_nanos = self.init_wait_nanos;

        // L01 node := CreateNode(value);
        let node_ptr = self.create_node(value);

        // L02 prev := DeRefLink(&head);
        let prev_ptr = self.bac_view.deref_link(self.head);

        // L03 next := DeRefLink(&prev.next);
        let prev = unsafe_mut_ref(&prev_ptr);
        let mut next_ptr = self.bac_view.deref_link(&prev.value_ref().next);

        // L04 while true do
        loop {
            // L05 StoreRef(&node.prev, <prev, false>));
            let node = unsafe_mut_ref(&node_ptr);
            self.bac_view
                .store_ref(&node.value_ref().prev, set_mark_raw(prev, false));

            // L06 StoreRef(&node.next, <next, false>));
            self.bac_view
                .store_ref(&node.value_ref().next, set_mark_raw(next_ptr, false));

            // L07 if CASRef(&prev.next, <next, false>, <node, false>) then break;
            if self.bac_view.cas_ref(
                &prev.value_ref().next,
                set_mark_raw(next_ptr, false),
                set_mark_raw(node_ptr, false),
            ) {
                break;
            }

            // L08 ReleaseRef(next);
            self.bac_view.release_ref(next_ptr);

            // L09 next := DeRefLink(&prev.next);
            next_ptr = self.bac_view.deref_link(&prev.value_ref().next);

            // L10 Back-Off
            self.back_off(&mut wait_nanos);
        }

        // L11 PushEnd(node, next);
        self.push_end(node_ptr, next_ptr);
    }

    /// ```text
    /// procedure PushRight(value: pointer to word)
    /// R01    node := CreateNode(value);
    /// R02    next := DeRefLink(&tail);
    /// R03    prev := DeRefLink(&next.prev);
    /// R04    while true do
    /// R05        StoreRef(&node.prev, (prev, false)));
    /// R06        StoreRef(&node.next, (next, false)));
    /// R07        if CASRef(&prev.next, (next, false), (node, false)) then break;
    /// R08        prev := CorrectPrev(prev, next);
    /// R09        Back-Off
    /// R10    PushEnd(node, next);
    /// ```
    fn push_right(&mut self, value: T) {
        let mut wait_nanos = self.init_wait_nanos;

        // R01 node := CreateNode(value);
        let node_ptr = self.create_node(value);

        // R02 next := DeRefLink(&tail);
        let next_ptr = self.bac_view.deref_link(self.tail);

        // R03 prev := DeRefLink(&next.prev);
        let next = unsafe_mut_ref(&next_ptr);
        let mut prev_ptr = self.bac_view.deref_link(&next.value_ref().prev);

        // R04 while true do
        loop {
            // R05 StoreRef(&node.prev, (prev, false)));
            let ptr = set_mark_raw(node_ptr, false);
            let node = unsafe_mut_ref(&ptr);
            self.bac_view
                .store_ref(&node.value_ref().prev, set_mark_raw(prev_ptr, false));

            // R06 StoreRef(&node.next, (next, false)));
            self.bac_view
                .store_ref(&node.value_ref().next, set_mark_raw(next_ptr, false));

            // R07 if CASRef(&prev.next, (next, false), (node, false)) then break;
            let ptr = set_mark_raw(prev_ptr, false);
            let prev = unsafe_mut_ref(&ptr);
            if self.bac_view.cas_ref(
                &prev.value_ref().next,
                set_mark_raw(next_ptr, false),
                set_mark_raw(node_ptr, false),
            ) {
                break;
            }

            // R08 prev := CorrectPrev(prev, next);
            prev_ptr = self.correct_prev(prev_ptr, next_ptr);

            // R09 Back-Off
            self.back_off(&mut wait_nanos);
        }

        // R10 PushEnd(node, next);
        self.push_end(node_ptr, next_ptr);
    }

    /// ```text
    /// function PopLeft(): pointer to word
    /// PL01    prev := DeRefLink(&head);
    /// PL02    while true do
    /// PL03        node := DeRefLink(&prev.next);
    /// PL04        if node = tail then
    /// PL05            ReleaseRef(node, prev);
    /// PL06            return NULL;
    /// PL07        next := DeRefLink(&node.next);
    /// PL08        if next.d = true then
    /// PL09            SetMark(&node.prev);
    /// PL10            CASRef(&prev.next, node, (next.p, false));
    /// PL11            ReleaseRef(next.p, node);
    /// PL12            continue;
    /// PL13        if CASRef(&node.next, next, (next.p, true)) then
    /// PL14            prev := CorrectPrev(prev, next);
    /// PL15            ReleaseRef(prev);
    /// PL16            value := node.value;
    /// PL17            break;
    /// PL18        ReleaseRef(next, node);
    /// PL19        Back-Off
    /// PL20    ReleaseRef(next, node);
    /// PL21    return value;
    /// ```
    fn pop_left(&mut self) -> Option<T> {
        let mut wait_nanos = self.init_wait_nanos;

        // PL01 prev := DeRefLink(&head);
        let mut prev_ptr = self.bac_view.deref_link(self.head);
        debug_assert!(!prev_ptr.is_null());

        // PL02 while true do
        loop {
            // PL03 node := DeRefLink(&prev.next);
            let prev = unsafe_mut_ref(&prev_ptr);
            debug_assert!(!prev.value_ref().next.load(Ordering::SeqCst).is_null());
            let node_ptr = self.bac_view.deref_link(&prev.value_ref().next);
            debug_assert!(!node_ptr.is_null());

            // PL04 if node = tail then
            if std::ptr::eq(node_ptr, self.tail.load(Ordering::SeqCst)) {
                // PL05 ReleaseRef(node, prev);
                self.bac_view.release_ref(node_ptr);
                self.bac_view.release_ref(prev_ptr);

                // PL06 return NULL;
                return None;
            }

            // PL07 next := DeRefLink(&node.next);
            let node = unsafe_mut_ref(&node_ptr);
            debug_assert!(!node.value_ref().next.load(Ordering::SeqCst).is_null());
            let next_ptr = self.bac_view.deref_link(&node.value_ref().next);
            debug_assert!(!next_ptr.is_null());

            // PL08 if next.d = true then
            if get_mark(next_ptr) {
                // PL09 SetMark(&node.prev);
                set_mark(&node.value_ref().prev, true);

                // PL10 CASRef(&prev.next, node, (next.p, false));
                self.bac_view.cas_ref(
                    &prev.value_ref().next,
                    node_ptr,
                    set_mark_raw(next_ptr, false),
                );

                // PL11 ReleaseRef(next.p, node);
                self.bac_view.release_ref(next_ptr);
                self.bac_view.release_ref(node_ptr);

                // PL12 continue;
                continue;
            }

            // PL13 if CASRef(&node.next, next, (next.p, true)) then
            if self.bac_view.cas_ref(
                &node.value_ref().next,
                next_ptr,
                set_mark_raw(next_ptr, true),
            ) {
                // PL14 prev := CorrectPrev(prev, next);
                prev_ptr = self.correct_prev(prev_ptr, next_ptr);

                // PL15 ReleaseRef(prev);
                self.bac_view.release_ref(prev_ptr);

                // PL16 value := node.value;
                let value = node.value_ref().value.clone();

                // PL17 break;
                // Because next_ptr and node_ptr go out of scope outside
                // this loop, we implement the break by releasing and returning
                // early.

                // PL20 ReleaseRef(next, node);
                self.bac_view.release_ref(next_ptr);
                self.bac_view.release_ref(node_ptr);

                // PL21 return value;
                return value;
            }

            // PL18 ReleaseRef(next, node);
            self.bac_view.release_ref(next_ptr);
            self.bac_view.release_ref(node_ptr);

            // PL19 Back-Off
            self.back_off(&mut wait_nanos);
        }
    }

    /// Note: [Sundell et. al. 2008] skips PR09, PR12, PR16, and PR20.
    ///
    /// ```text
    /// function PopRight(): pointer to word
    /// PR01    next := DeRefLink(&tail);
    /// PR02    node := DeRefLink(&next.prev);
    /// PR03    while true do
    /// PR04        if node.next != (next, false) then
    /// PR05            node := CorrectPrev(node, next);
    /// PR06            continue;
    /// PR07        if node = head then
    /// PR08            ReleaseRef(node, next);
    /// PR10            return NULL;
    /// PR11        if CASRef(&node.next, (next, false), (next, true)) then
    /// PR13            prev := DeRefLink(&node.prev);
    /// PR14            prev := CorrectPrev(prev, next);
    /// PR15            ReleaseRef(prev, next);
    /// PR17            value := node.value;
    /// PR18            break;
    /// PR19        Back-Off
    /// PR21    ReleaseRef(node);
    /// PR22    return value;
    /// ```
    fn pop_right(&mut self) -> Option<T> {
        let mut wait_nanos = self.init_wait_nanos;

        // PR01 next := DeRefLink(&tail);
        let next_ptr = self.bac_view.deref_link(self.tail);

        // PR02 node := DeRefLink(&next.prev);
        let next = unsafe_mut_ref(&next_ptr);
        let mut node_ptr = self.bac_view.deref_link(&next.value_ref().prev);

        // PR03 while true do
        loop {
            // PR04 if node.next != (next, false) then
            let node = unsafe_mut_ref(&node_ptr);
            if !std::ptr::eq(
                node.value_ref().next.load(Ordering::SeqCst),
                set_mark_raw(next_ptr, false),
            ) {
                // PR05 node := CorrectPrev(node, next);
                node_ptr = self.correct_prev(node_ptr, next_ptr);

                // PR06 continue;
                continue;
            }

            // PR07 if node = head then
            if std::ptr::eq(node_ptr, self.head.load(Ordering::SeqCst)) {
                // PR08 ReleaseRef(node, next);
                self.bac_view.release_ref(node_ptr);
                self.bac_view.release_ref(next_ptr);

                // PR10 return NULL;
                return None;
            }

            // PR11 if CASRef(&node.next, (next, false), (next, true)) then
            let b = self.bac_view.cas_ref(
                &node.value_ref().next,
                set_mark_raw(next_ptr, false),
                set_mark_raw(next_ptr, true),
            );
            if b {
                // PR13 prev := DeRefLink(&node.prev);
                let prev = self.bac_view.deref_link(&node.value_ref().prev);

                // PR14 prev := CorrectPrev(prev, next);
                let prev = self.correct_prev(prev, next_ptr);

                // PR15 ReleaseRef(prev, next);
                self.bac_view.release_ref(set_mark_raw(prev, false));

                // PR17 value := node.value;
                let node = unsafe_mut_ref(&node_ptr);
                let value = node.value_ref().value.clone();

                // PR18 break;
                // Because next_ptr and node_ptr go out of scope outside
                // this loop, we implement the break by releasing and returning
                // early.

                // PR21 ReleaseRef(node);
                self.bac_view.release_ref(node_ptr);

                // PR22 return value;
                return value;
            }
            // PR19 Back-Off
            self.back_off(&mut wait_nanos);
        }
    }

    /// ```text
    /// function Delete(cursor: pointer to pointer to Node): pointer to word
    /// D01    node := *cursor;
    /// D02    if node = head or node = tail then return NULL;
    /// D03    while true do
    /// D04        next := DeRefLink(&(*cursor).next);
    /// D05        if next.d = true then
    /// D06            ReleaseRef(next);
    /// D07            return NULL;
    /// D08         if CAS(&node.next, next, (next.p, true)) then
    /// D09             while true do
    /// D10                 prev := DeRefLink(&node.prev);
    /// D11                 if prev.d = true or
    /// ...                        CAS(&node.prev, prev, (prev.p, true))
    /// ...                           then break;
    /// D12             prev := CorrectPrev(prev.p, next);
    /// D13             ReleaseRef(prev); ReleaseRef(next);
    /// D14             value := node.value;
    /// D15             ReleaseRef(node); DeleteNode(node);
    /// D16             return value;
    /// ```
    fn delete(&mut self, cursor: *mut *mut Node<ListProps<T>>) -> Option<T> {
        // D01 node := *cursor;
        let node_ptr: *mut Node<ListProps<T>> = unsafe { *cursor };

        // D02 if node = head or node = tail then return NULL;
        if std::ptr::eq(node_ptr, self.head.load(Ordering::SeqCst))
            || std::ptr::eq(node_ptr, self.tail.load(Ordering::SeqCst))
        {
            return None;
        }

        // D03 while true do
        loop {
            // D04 next := DeRefLink(&(*cursor).next);
            let next_ptr = self
                .bac_view
                .deref_link(&unsafe { &**cursor }.value_ref().next);

            // D05 if next.d = true then
            if get_mark(next_ptr) {
                // D06 ReleaseRef(next);
                self.bac_view.release_ref(next_ptr);

                // D07 return NULL;
                return None;
            }

            // D08 if CAS(&node.next, next, (next.p, true)) then
            let node = unsafe_mut_ref(&node_ptr);
            let b = self.bac_view.cas_ref(
                &node.value_ref().next,
                next_ptr,
                set_mark_raw(next_ptr, true),
            );
            if b {
                // `prev_ptr` needs to outlive the scope of the following loop.
                let mut prev_ptr;

                // D09 while true do
                loop {
                    // D10 prev := DeRefLink(&node.prev);
                    let node = unsafe_mut_ref(&node_ptr);
                    prev_ptr = self.bac_view.deref_link(&node.value_ref().prev);

                    // D11 if prev.d = true or
                    // ... CAS(&node.prev, prev, (prev.p, true))
                    // ... then break;
                    let b = get_mark(prev_ptr)
                        || self.bac_view.cas_ref(
                            &node.value_ref().prev,
                            prev_ptr,
                            set_mark_raw(prev_ptr, true),
                        );
                    if b {
                        break;
                    }
                }

                // D12 prev := CorrectPrev(prev.p, next);
                let prev_ptr = self.correct_prev(set_mark_raw(prev_ptr, false), next_ptr);

                // D13 ReleaseRef(prev); ReleaseRef(next);
                self.bac_view.release_ref(prev_ptr);
                self.bac_view.release_ref(next_ptr);

                // D14 value := node.value;
                let node = unsafe_mut_ref(&node_ptr);
                let value = node.value_ref().value.clone();

                // D15 ReleaseRef(node); DeleteNode(node);
                // Note: While the paper says to release the reference before
                // deleting the node, the [delete_node] method already releases
                // the reference. Presumably, [delete_node] requires that the
                // reference be the last reference to the node. However, it
                // seems the [Sundell & Tsigas 2008] mistakenly thought
                // [delete_node] only performed the cleanup for the Beware and
                // Cleanup garbage collector's internal state.
                self.bac_view.delete_node(node_ptr);

                // D16 return value;
                return value;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // TODO(t/1594): This should be 100;
    const TOP: usize = 10;

    #[test]
    fn test_simple_push_left() {
        let list: List<u32> = List::new();
        let mut list_view = list.view(0);
        assert!(list_view.pop_left().is_none());
        list_view.push_left(1);

        // manually check values
        let head = list_view.head.load(Ordering::SeqCst);
        assert!(!head.is_null());
        let head = unsafe { &*head };
        let head_value = head.value_ref().value;
        assert!(head_value.is_none());
        let next = head.value_ref().next.load(Ordering::SeqCst);
        assert!(!next.is_null());
        let next = unsafe { &*next };
        let next_value = next.value_ref().value;
        assert!(next_value.is_some());

        // check via supported API
        assert_eq!(1, list_view.pop_left().unwrap());
    }

    fn simple_list(top: usize) -> List<usize> {
        let list: List<usize> = List::new();
        let mut list_view = list.view(0);

        for i in 0..top {
            list_view.push_left(i);
        }
        for i in 0..top {
            list_view.push_right(i + top);
        }
        list
    }

    #[test]
    fn test_list_pop() {
        let list = simple_list(TOP);
        let mut list_view = list.view(1);
        for i in 0..TOP {
            assert_eq!(TOP - i - 1, list_view.pop_left().unwrap());
        }
        for i in 0..TOP {
            assert_eq!(2 * TOP - i - 1, list_view.pop_right().unwrap());
        }
        assert!(list_view.pop_left().is_none());
        assert!(list_view.pop_right().is_none());
    }

    #[test]
    fn test_push_pop() {
        let top_2 = TOP * TOP;

        // Construct list of values to push.
        let mut push_values: Vec<i32> = Vec::with_capacity(top_2);
        let mut next_value = 0;
        for _ in 0..top_2 {
            push_values.push(next_value - 27);
            next_value += 19;
            next_value %= 53;
        }

        let mut tracking_vec: Vec<i32> = Vec::with_capacity(top_2);

        let list = List::new();
        let mut list_view = list.view(0);

        for i in 0..top_2 {
            let should_push = i % 2 == 0;
            let handle_left = i % 3 == 0 || i % 5 == 0;
            if should_push {
                let next = push_values.pop().unwrap();
                if handle_left {
                    tracking_vec.insert(0, next);
                    list_view.push_left(next);
                    continue;
                }
                tracking_vec.push(next);
                list_view.push_right(next);
                continue;
            }

            // should pop
            assert!(!should_push);
            let (list_value, vec_value) = if handle_left {
                let vec_value = if !tracking_vec.is_empty() {
                    Some(tracking_vec.remove(0))
                } else {
                    None
                };
                (list_view.pop_left(), vec_value)
            } else {
                (list_view.pop_right(), tracking_vec.pop())
            };
            assert_eq!(list_value, vec_value);
        }

        while !tracking_vec.is_empty() {
            assert_eq!(list_view.pop_right(), tracking_vec.pop());
        }

        assert!(list_view.pop_left().is_none());
        assert!(list_view.pop_right().is_none());
    }

    #[test]
    fn test_simple_delete() {
        for i in 0..2 {
            // Construct list of the form [-1, 0, 1].
            let list = List::new();
            let mut list_view = list.view(0);
            list_view.push_right(0);
            list_view.push_left(-1);
            list_view.push_right(1);

            // Sanity check the head sentinel node contains no value.
            let node_ptr = list_view.head.load(Ordering::SeqCst);
            let node = unsafe { &mut *node_ptr };
            assert!(node.value_ref().value.is_none());

            // Sanity check the first node contains -1.
            let node_ptr = node.value_ref().next.load(Ordering::SeqCst);
            let node = unsafe { &mut *node_ptr };
            assert_eq!(node.value_ref().value, Some(-1));

            // Sanity check the middle node contains 0.
            let node_ptr = node.value_ref().next.load(Ordering::SeqCst);
            let node = unsafe { &mut *node_ptr };
            assert_eq!(node.value_ref().value, Some(0));

            // Delete the middle node.
            let boxed = Box::new(node_ptr);
            let cursor_ptr = Box::into_raw(boxed);
            list_view.delete(cursor_ptr);

            // Check the values of the list after deletion.
            let pop_from_left = i % 2 == 0;
            if pop_from_left {
                assert_eq!(list_view.pop_left(), Some(-1));
                assert_eq!(list_view.pop_left(), Some(1));
                assert!(list_view.pop_left().is_none());
                assert!(list_view.pop_right().is_none());
                continue;
            }
            assert_eq!(list_view.pop_right(), Some(1));
            assert_eq!(list_view.pop_right(), Some(-1));
            assert!(list_view.pop_left().is_none());
            assert!(list_view.pop_right().is_none());
        }
    }

    #[test]
    fn test_concurrent_push_pop_1() {
        let list: List<usize> = List::new();

        let modulus = 7;
        let remainders = [2, 3, 5];
        for remainder in remainders {
            assert!(remainder < modulus);
        }

        std::thread::scope(|s| {
            let mut list_views = Vec::with_capacity(remainders.len());
            for thread_id in 0..remainders.len() {
                list_views.push(list.view(thread_id));
            }
            let last_list_view = list.view(remainders.len());
            let mut join_handles = Vec::new();
            for (remainder, mut list_view) in remainders.iter().zip(list_views.into_iter()) {
                let remainder = *remainder;
                let handle = s.spawn(move || {
                    for i in 0..TOP {
                        list_view.push_right(i * modulus + remainder);
                    }
                });
                join_handles.push(handle);
            }

            for handle in join_handles {
                handle.join().unwrap();
            }

            let mut list_view = last_list_view;
            let mut found_count = 0;
            let mut last_seen: Vec<Option<usize>> = vec![None; remainders.len()];
            while found_count < remainders.len() * TOP {
                let seen = if let Some(value) = list_view.pop_right() {
                    value
                } else {
                    continue;
                };
                found_count += 1;
                let mut ok = false;
                for (idx, remainder) in remainders.iter().enumerate() {
                    if seen % modulus != *remainder {
                        continue;
                    }
                    if let Some(last) = last_seen[idx] {
                        assert!(seen < last);
                    }
                    last_seen[idx] = Some(seen);
                    ok = true;
                    break;
                }
                assert!(ok);
            }
        });
    }

    #[test]
    fn test_concurrent_push_pop_2() {
        let list: List<usize> = List::new();

        let modulus = 7;
        let remainders = [2, 3, 5];
        for remainder in remainders {
            assert!(remainder < modulus);
        }

        let mut expected = std::collections::HashMap::new();
        for remainder in &remainders {
            for i in 0..TOP {
                expected.insert(i * modulus + remainder, false);
            }
        }

        std::thread::scope(|s| {
            let mut list_views = Vec::with_capacity(remainders.len());
            for thread_id in 0..remainders.len() {
                list_views.push(list.view(thread_id));
            }
            let last_list_view = list.view(remainders.len());
            let mut join_handles = Vec::new();
            for (remainder, mut list_view) in remainders.iter().zip(list_views.into_iter()) {
                let remainder = *remainder;
                let handle = s.spawn(move || {
                    for i in 0..TOP {
                        let value = i * modulus + remainder;
                        list_view.push_right(value);
                    }
                });
                join_handles.push(handle);
            }

            let mut list_view = last_list_view;
            let mut found_count = 0;
            while found_count < remainders.len() * TOP {
                let seen = if let Some(value) = list_view.pop_right() {
                    value
                } else {
                    continue;
                };
                found_count += 1;
                assert!(expected.contains_key(&seen));
                assert!(!expected[&seen]);
                *expected.get_mut(&seen).unwrap() = true;
            }

            for (_, value) in expected {
                assert!(value);
            }

            for handle in join_handles {
                handle.join().unwrap();
            }
        });
    }
}
