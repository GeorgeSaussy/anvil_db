use std::fmt::Debug;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::Arc;

use crate::kv::TombstonePairLike;
use crate::MarkingPtr;
use crate::RangeSet;

// TODO(t/1683): Is Ordering::SeqCst too strong?
const ORDERING: AtomicOrdering = AtomicOrdering::SeqCst;

#[derive(Debug)]
struct LockableUsize {
    value: AtomicUsize,
}

impl LockableUsize {
    fn locked(&self) -> bool {
        // Relaxed reads are okay because the SL algorithms expect
        // potentially stale reads.
        let x = self.value.load(AtomicOrdering::Relaxed);
        x >> (std::mem::size_of::<usize>() - 1) == 1
    }

    /// Returns the value of the usize when it was locked.
    fn lock(&self) -> usize {
        let w = std::mem::size_of::<usize>();
        loop {
            let old = self.value.load(AtomicOrdering::Relaxed);
            if old >> (w - 1) == 1 {
                continue;
            }
            let new = old | (1 << (w - 1));
            if self
                .value
                .compare_exchange(old, new, AtomicOrdering::Relaxed, AtomicOrdering::Relaxed)
                .is_ok()
            {
                return old;
            }
            std::thread::yield_now();
        }
    }

    fn unlock(&self, new_value: usize) {
        let w = std::mem::size_of::<usize>();
        debug_assert_eq!(new_value >> (w - 1), 0);
        debug_assert_eq!(self.value.load(AtomicOrdering::Relaxed) >> (w - 1), 1);
        self.value.store(new_value, AtomicOrdering::Relaxed);
    }

    fn read(&self) -> usize {
        let x = self.value.load(AtomicOrdering::Relaxed);
        x & !(1 << (std::mem::size_of::<usize>() - 1))
    }
}

struct ValueHolder {
    value: Option<Vec<u8>>,
    prev_value: AtomicPtr<ValueHolder>,
    dirty: AtomicBool,
}

#[derive(Debug)]
struct Node {
    forward: Vec<MarkingPtr<Node>>,
    key: Vec<u8>,
    // TODO(t/1692): This should be a Vec<u8>, but that would require that
    // values not be updated, which would require version info be stored in the
    // keys.
    value: AtomicPtr<ValueHolder>,
}

unsafe impl Send for Node {}

impl Node {
    fn replace_value(&self, new_value: Option<&[u8]>) {
        let holder = Box::into_raw(Box::new(ValueHolder {
            value: new_value.map(|v| v.to_vec()),
            prev_value: AtomicPtr::new(std::ptr::null_mut()),
            dirty: AtomicBool::new(true),
        }));
        let prev = self.value.swap(holder, ORDERING);
        (unsafe { &*holder }).prev_value.store(prev, ORDERING);
        (unsafe { &*holder }).dirty.store(false, ORDERING);
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
struct NodePtrView(*const Node);

impl From<*const Node> for NodePtrView {
    fn from(node_ptr: *const Node) -> Self {
        NodePtrView(node_ptr)
    }
}

impl NodePtrView {
    fn new(new_max_level: usize, search_key: &[u8], new_value: Option<&[u8]>) -> Self {
        // put node on the heap
        let forward: Vec<_> = (0..(new_max_level + 1))
            .map(|_| MarkingPtr::null())
            .collect();
        let value = AtomicPtr::new(Box::into_raw(Box::new(ValueHolder {
            value: new_value.map(|v| v.to_vec()),
            prev_value: AtomicPtr::new(std::ptr::null_mut()),
            dirty: AtomicBool::new(false),
        })));
        let node_box = Box::into_raw(Box::new(Node {
            forward,
            key: search_key.to_vec(),
            value,
        }));
        NodePtrView(node_box)
    }

    fn raw(&self) -> *const Node {
        let NodePtrView(ptr) = self;
        *ptr
    }

    fn node_ref(&self) -> &Node {
        let ptr = self.raw();
        debug_assert!(!ptr.is_null());
        (unsafe { &*ptr }) as _
    }

    fn get_forward(&self, level: usize) -> NodePtrView {
        let node = self.node_ref();
        let forward_ref = &node.forward[level];
        NodePtrView::from(forward_ref.ptr_value(ORDERING))
    }

    fn set_forward(&self, level: usize, new_fwd: *mut Node) {
        let node = self.node_ref();
        let forward_ref = &node.forward[level];
        forward_ref.set_ptr_value(new_fwd, ORDERING);
    }

    fn is_end(&self) -> bool {
        self.raw().is_null() || self.raw().is_null()
    }

    fn read_key(&self) -> &[u8] {
        let node = self.node_ref();
        &node.key
    }

    fn read_value(&self) -> Option<&Vec<u8>> {
        let node = self.node_ref();
        let value_ptr = node.value.load(ORDERING);
        if value_ptr.is_null() {
            return None;
        }
        let holder = unsafe { &*value_ptr };
        holder.value.as_ref()
    }

    fn max_level(&self) -> usize {
        let node = self.node_ref();
        debug_assert!(!node.forward.is_empty());
        node.forward.len() - 1
    }
}

/// This implementation is based on the paper
/// "Concurrent Maintenance of Skip Lists" by William Pugh.
/// However, because we will never remove a node from the list, we do not
/// need to support the `remove` operation. In addition, because we know the
/// type of the data that will be stored, the we can serialize the nodes
/// in a flat memory pool.
/// WARNING: Caller must put this behind an Arc for concurrent access.
pub(crate) struct SkipList {
    /// The highest level to start the search at.
    level_hint: LockableUsize,
    level_cap: usize,
    /// The forward pointers in the head. A null pointer value in the head
    /// is the same as pointing at the end block. The length of
    /// [SkipList::head] is [Self::level_cap].
    head: NodePtrView,
    /// Used for random level generation.
    /// This uses a XORShift random number generator.
    /// TODO(anvil_db): Perhaps this should read from OS randomness.
    xs: AtomicU64,
}

unsafe impl Send for SkipList {}
unsafe impl Sync for SkipList {}

impl std::fmt::Debug for SkipList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SkipList")
            .field("level_hint", &self.level_hint)
            .field("level_cap", &self.level_cap)
            .field("xs", &self.xs)
            .finish()
    }
}

impl Default for SkipList {
    fn default() -> Self {
        let level_cap = 32;
        Self::new(level_cap)
    }
}

impl Drop for SkipList {
    fn drop(&mut self) {
        let mut ptr = self.head;
        while !ptr.is_end() {
            let next = ptr.get_forward(0);
            let _ = unsafe { Box::from_raw(ptr.raw() as *mut Node) };
            if next.is_end() {
                break;
            }
            ptr = next;
        }
    }
}

impl SkipList {
    /// `level_cap` is the maximum level of the skip list.
    /// There will be `level_cap + 1` levels in the skip list because
    /// levels are indexed from zero.
    /// NOTE:
    pub(crate) fn new(max_level: usize) -> Self {
        // level is always encoded as a u8, so a higher cap cannot be used.
        debug_assert!(max_level < 256);
        // check full pool size can be encoded

        let level_hint = LockableUsize {
            value: AtomicUsize::new(0),
        };

        let forward: Vec<_> = (0..(max_level + 1)).map(|_| MarkingPtr::null()).collect();
        let head = NodePtrView(Box::into_raw(Box::new(Node {
            forward,
            key: vec![],
            value: AtomicPtr::new(std::ptr::null_mut()),
        })));

        SkipList {
            level_hint,
            level_cap: max_level,
            head,
            xs: AtomicU64::new(1337),
        }
    }

    fn lock(&self, node_view: NodePtrView, level: usize) {
        let node = node_view.node_ref();
        // Loop until previous mark value was false.
        while node.forward[level].set_mark(true, ORDERING) {
            std::thread::yield_now();
        }
        debug_assert!(node.forward[level].get_mark(ORDERING));
        debug_assert!(
            self.locked(node_view, level),
            "{:?} {} {:?}",
            node_view,
            level,
            node.forward[level]
        );
    }

    fn unlock(&self, node_view: NodePtrView, level: usize) {
        debug_assert!(self.locked(node_view, level));
        let node = node_view.node_ref();
        let fwd = &node.forward[level];
        let prev = fwd.set_mark(false, ORDERING);
        debug_assert!(prev, "{:?} {} {:?}", node_view, level, node.forward[level]);
    }

    fn locked(&self, node_view: NodePtrView, level: usize) -> bool {
        let node = node_view.node_ref();
        node.forward[level].get_mark(ORDERING)
    }

    /// The function [NodePtrExt::get_lock] is based on the pseudocode for
    /// `getLock` in Figure 7 on pg. 8 of the paper.
    fn get_lock(&self, node_ptr: NodePtrView, search_key: &[u8], level: usize) -> NodePtrView {
        debug_assert!(!node_ptr.is_end());
        debug_assert!(node_ptr == self.head || node_ptr.read_key() < search_key);

        // > 01 | -- x is at least level i and x->key < searchKey
        //
        // Here we use `self` as `x`, and `level` as `i`.
        debug_assert!(node_ptr == self.head || node_ptr.max_level() >= level);

        // > 02 | -- since some time might have elapsed since we tried to
        // > .. |    advance at this level, see if we can advance further.
        // > 03 | y := x -> forward[i]
        let mut x = node_ptr;
        let mut y = self.get_forward(x, level);

        // > 04 | while y->key < searchKey do
        // > 05 |     x := y
        // > 06 |     y := x -> forward[i]
        loop {
            debug_assert_ne!(y, self.head);
            if y.is_end() {
                // the end node
                break;
            }
            if y.read_key() >= search_key {
                break;
            }
            x = y;
            y = self.get_forward(x, level);
        }

        // > 07 | -- can't move any further, lock and double check result
        // > 08 | lock(x, forward[i])
        // > 09 | y := x -> forward[i]
        // > 10 | while y->key < searchKey do
        // > 11 |     unlock(x, forward[i])
        // > 12 |     x := y
        // > 13 |     lock(x, forward[i])
        // > 14 |     y := x -> forward[i]
        self.lock(x, level);
        y = self.get_forward(x, level);
        loop {
            debug_assert_ne!(y, self.head);
            if y.is_end() {
                // the end node
                break;
            }
            let found_key = y.read_key();
            if found_key >= search_key {
                break;
            }
            self.unlock(x, level);
            x = y;
            self.lock(x, level);
            y = self.get_forward(x, level);
        }

        // > 15 | -- locked(x->forward[i]) and
        // > .. |    x->key < searchKey <= key(x->forward[i])
        // > .. |    x is reachable at level i
        debug_assert!(self.locked(x, level), "{x:?} {level}");
        debug_assert!(!x.is_end());
        debug_assert!(x == self.head || x.read_key() < search_key);
        #[cfg(test)]
        {
            let y = self.get_forward(x, level);
            debug_assert_ne!(y, self.head);
            debug_assert!(y.is_end() || search_key <= y.read_key());
        }
        debug_assert!(self.node_level(x) >= level);
        x
    }

    fn node_level(&self, node_ptr: NodePtrView) -> usize {
        node_ptr.max_level()
    }

    fn get_forward(&self, node_ptr: NodePtrView, level: usize) -> NodePtrView {
        node_ptr.get_forward(level)
    }

    fn set_forward(&self, node_ptr: NodePtrView, level: usize, new_fwd: *mut Node) {
        debug_assert!(!node_ptr.is_end());
        debug_assert!(node_ptr.max_level() >= level);
        node_ptr.set_forward(level, new_fwd);
    }

    fn assert_null_set_forward(&self, node_ptr: NodePtrView, level: usize, new_fwd: *mut Node) {
        debug_assert!(!node_ptr.is_end());
        debug_assert!(node_ptr.max_level() >= level);
        debug_assert!(node_ptr.get_forward(level).raw().is_null());
        node_ptr.set_forward(level, new_fwd);
    }

    /// Returns `Some(None)` for a deletion marker, `Some(Some(value))` for a
    /// value, and `None` if the key could not be found.
    /// This is based on the pseudo-code for `Search(list, searchKey)` in
    /// Figure 7 on pg. 8 of the paper.
    pub(crate) fn search(&self, search_key: &[u8]) -> Option<Option<*const Vec<u8>>> {
        // > 01 | x := list->header
        let mut x_view = self.head;

        // `y_ptr` needs to be scoped outside the following loop.
        let mut y_view = NodePtrView(std::ptr::null());

        // > 02 | for i := list->levelHint downto 1 do
        // > 03 |     y := x->forward[i]
        // > 04 |     while y->key < searchKey do
        // > 05 |         x := y
        // > 06 |         y := x->forward[i]
        for level in (0..self.level_hint.read() + 1).rev() {
            y_view = self.get_forward(x_view, level);
            while !y_view.is_end() && y_view.read_key() < search_key {
                x_view = y_view;
                y_view = self.get_forward(x_view, level);
            }
        }

        // > 07 | -- wasTrue(self, y = x->forward[i]) &&
        // > .. |    x->key < searchKey <= y->key
        //
        // These cannot be checked without a lock. Concurrent updates mean that
        // a debug_assert could fail here even if it were true during the
        // previous loop. Locking later in this function makes similar checks
        // safe later.
        debug_assert!(x_view == self.head || x_view.read_key() < search_key);

        // > 08 | if y->key = searchKey then return y->value
        // > 09 | else return not-found
        if y_view.is_end() || y_view.read_key() != search_key {
            return None;
        }
        Some(y_view.read_value().map(std::ptr::from_ref))
    }

    /// Inserts a key-value pair into the skip list.
    /// Returns `true` if the value was updated in place, and
    /// `false` if the key was inserted.
    /// See the `insert` algorithm on in Figure 8 on pg. 12 of the paper.
    pub(crate) fn insert(&self, search_key: &[u8], new_value: Option<&[u8]>) -> bool {
        // > 01 | local update[1..levelCap]
        // > 02 | x = list->header
        // > 03 | L = list->levelHint
        //
        // In this implementation we use `update_ptrs` instead of `update`;
        // `node_ptr` instead of `x`; and `head_hint` instead of `L`.
        let level_hint = self.level_hint.read();
        let mut update_idxs: Vec<NodePtrView> = (0..self.level_cap + 1)
            .map(|_| NodePtrView(std::ptr::null()))
            .collect();
        // The header is at index 1.
        let mut x_view = self.head;

        // > 05 | for i = L downto 1 do
        // > 06 |    y = x->forward[i]
        // > 07 |    while y->key < searchKey do
        // > 08 |        x = y
        // > 09 |        y = x->forward[i]
        // > 10 |    update[i] = x
        //
        // Instead of using index variable `i`, we use `level`. This convention
        // is used throughout the `insert` implementation.
        for level in (0..(level_hint + 1)).rev() {
            let mut y_view = self.get_forward(x_view, level);
            'inner: loop {
                if y_view.is_end() {
                    // the end node
                    break 'inner;
                }
                if y_view != self.head && y_view.read_key() >= search_key {
                    // an inner node
                    break 'inner;
                }
                x_view = y_view;
                y_view = self.get_forward(y_view, level);
                let node_level = if y_view.is_end() {
                    999
                } else {
                    self.node_level(y_view)
                };
                debug_assert!(
                    y_view.is_end() || node_level >= level,
                    "{:?} {} {} {:?}",
                    y_view,
                    node_level,
                    level,
                    y_view.read_key()
                );
            }
            update_idxs[level] = x_view;
        }

        // > 11 | x = getLock(x, searchKey, 1)
        x_view = self.get_lock(x_view, search_key, 0);

        // > 12 | if x->forward[1]->key == searchKey then
        // > 13 |     x->forward[1]->value = newValue
        // > 14 |     unlock(x, forward[1])
        // > 15 |     return updated
        let next_view = self.get_forward(x_view, 0);
        if !next_view.is_end() && next_view != self.head {
            // inner node
            let found_key = next_view.read_key();
            if found_key == search_key {
                let node = next_view.node_ref();
                node.replace_value(new_value);
                self.unlock(x_view, 0);
                return true;
            }
        }

        // > 16 | y = makeNode(randomLevel(), searchKey, value)
        // > 17 | lock(y, level)
        //
        // Note: there there is no `level` variable in scope. From context,
        // this variable is the level of the new node.
        let new_max_level = self.random_level();
        let y_view = NodePtrView::new(new_max_level, search_key, new_value);
        self.lock(y_view, new_max_level);

        // > 18 | -- if L < y->level, add levels L+1 to update, so that we can insert y
        // > .. |    into levels 1..y->level.
        // > 19 | -- This will allow the header of the list ot be updated correctly.
        // > 20 | for i = L+1 to y->level do update[i] = header(list)
        for update_idx_ref in update_idxs
            .iter_mut()
            .take(new_max_level + 1)
            .skip(level_hint + 1)
        {
            *update_idx_ref = self.head;
        }

        // > 21 | -- insert y into levels 1..level(y)
        // > 22 | for i = 1 to y->level do
        // > 23 |     -- y is effectively a level i-1 element
        // > 24 |     if i != 1 then x = getLock(update[i], searchKey, i)
        // > 25 |     -- searchKey is not present at level i,
        // > 26 |        therefore x->key < searchKey < x->forward[i]->key
        // > 27 |     y->forward[i] = x->forward[i]
        // > 28 |     x->forward[i] = y
        // > 29 |     unlock(x, forward[i])
        // > 30 | unlock(y, level)
        for (level, update_idx) in update_idxs.iter().enumerate().take(new_max_level + 1) {
            debug_assert!(level <= self.node_level(y_view));
            if level != 0 {
                x_view = self.get_lock(*update_idx, search_key, level);
            }
            debug_assert!(
                self.locked(x_view, level),
                "x_view {:?} level {}, node {:?}",
                x_view,
                level,
                x_view.node_ref()
            );
            let fwd = self.get_forward(x_view, level);
            self.assert_null_set_forward(y_view, level, fwd.raw() as *mut Node);
            debug_assert!(
                self.locked(x_view, level),
                "x_view {:?} level {}, node {:?}",
                x_view,
                level,
                x_view.node_ref()
            );
            self.set_forward(x_view, level, y_view.raw() as *mut Node);
            debug_assert!(
                self.locked(x_view, level),
                "x_view {x_view:?} level {level}"
            );
            debug_assert!(
                self.locked(x_view, level),
                "v_view {x_view:?} level {level}"
            );
            self.unlock(x_view, level);
        }

        self.unlock(y_view, new_max_level);

        // > 31 | -- increase levelHint to correspond to maximum non-NIL level
        // > .. |    if needed
        // > 32 | L = list->levelHint
        let head_hint = self.level_hint.read();

        // > 33 | if L < levelCap and list->header->forward[L+1] != NIL
        // > .. |     and locked(list, levelHint)
        // > .. | then
        // > 34 |     lock(list, levelHint)
        // > 35 |     while list->levelHint < levelCap
        // > .. |         and list->header->forward[list->levelHint+1] != NIL
        // > .. |     do
        // > 36 |         list->levelHist = list->levelHint + 1
        // > 37 |     unlock(list, levelHint)
        // > 38 | return inserted
        if self.level_cap <= head_hint
            || self.get_forward(self.head, head_hint).is_end()
            || self.level_hint.locked()
        {
            return false;
        }
        let mut new_level_hint = self.level_hint.lock();
        while new_level_hint < self.level_cap
            && !self.get_forward(self.head, new_level_hint + 1).is_end()
        {
            new_level_hint += 1;
        }
        self.level_hint.unlock(new_level_hint);

        false
    }

    /// Generate a random number.
    fn gen(&self) -> u64 {
        loop {
            // TODO(t/1683): Is Relaxed okay?
            let loaded = self.xs.load(AtomicOrdering::Relaxed);
            let mut x = loaded;
            if x == 0 {
                x = 1;
            }
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            if self
                .xs
                .compare_exchange(loaded, x, AtomicOrdering::SeqCst, AtomicOrdering::SeqCst)
                .is_ok()
            {
                return x;
            }
        }
    }

    fn random_level(&self) -> usize {
        let p = 0.5; // TODO(OMEGA-937): This should be tunable.
        let mut lvl = 0;
        let accuracy = 10_000;
        // TODO(gs): Write a better random library.
        while ((self.gen() % accuracy) as f64) < p * (accuracy as f64) && lvl < self.level_cap - 1 {
            lvl += 1;
        }
        lvl
    }
}

struct NodePtrWrapper {
    ptr: *const Node,
}

unsafe impl Send for NodePtrWrapper {}
unsafe impl Sync for NodePtrWrapper {}

pub(crate) struct SkipListScanner {
    _drop_preventer: Arc<SkipList>,
    low: Option<Vec<u8>>,
    high: Option<Vec<u8>>,
    inner: NodePtrWrapper,
}

impl From<Arc<SkipList>> for SkipListScanner {
    fn from(list: Arc<SkipList>) -> Self {
        SkipListScanner {
            _drop_preventer: list.clone(),
            low: None,
            high: None,
            inner: NodePtrWrapper {
                ptr: list.head.raw(),
            },
        }
    }
}

/// Note: [SkipListScanItem] will be invalid if the scanner is dropped.
pub(crate) struct SkipListScanItem {
    inner: *const Node,
}

impl TombstonePairLike for SkipListScanItem {
    fn key_ref(&self) -> &[u8] {
        debug_assert!(!self.inner.is_null());
        let node = unsafe { &*self.inner };
        &node.key
    }

    fn value_ref(&self) -> Option<&[u8]> {
        let node = unsafe { &*self.inner };
        let value_ptr = node.value.load(ORDERING);
        if value_ptr.is_null() {
            return None;
        }
        let holder = unsafe { &*value_ptr };
        holder.value.as_deref()
    }
}

impl Iterator for SkipListScanner {
    type Item = SkipListScanItem;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.inner.ptr.is_null() {
                return None;
            }
            self.inner.ptr = unsafe { &*(self.inner.ptr) }.forward[0].ptr_value(ORDERING);
            if self.inner.ptr.is_null() {
                return None;
            }

            let item = SkipListScanItem {
                inner: self.inner.ptr,
            };
            if let Some(low) = &self.low {
                if item.key_ref() < low {
                    continue;
                }
            }
            if let Some(high) = &self.high {
                if item.key_ref() > high {
                    return None;
                }
            }
            return Some(item);
        }
    }
}

impl RangeSet for SkipListScanner {
    fn from(mut self, key: &[u8]) -> Self {
        self.low = Some(key.to_vec());
        self
    }

    fn to(mut self, key: &[u8]) -> Self {
        self.high = Some(key.to_vec());
        self
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, RwLock};

    use super::*;
    use crate::helpful_macros::{spawn, unwrap};
    use crate::logging::debug;

    extern crate test;
    use test::Bencher;

    trait Store: Clone + Send {
        /// Returns `true` if the key was updated in place, and `false` if
        /// was inserted.
        fn insert(&mut self, key: &[u8], value: &[u8]) -> bool;
        fn search(&self, key: &[u8]) -> Option<Vec<u8>>;
    }

    impl Store for Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>> {
        fn insert(&mut self, key: &[u8], value: &[u8]) -> bool {
            let mut map = unwrap!(self.write());
            map.insert(key.to_vec(), value.to_vec()).is_some()
        }

        fn search(&self, key: &[u8]) -> Option<Vec<u8>> {
            let map = unwrap!(self.read());
            map.get(key).map(|v| v.as_slice().to_vec())
        }
    }

    impl Store for Arc<SkipList> {
        fn insert(&mut self, key: &[u8], value: &[u8]) -> bool {
            SkipList::insert(self, key, Some(value))
        }

        fn search(&self, key: &[u8]) -> Option<Vec<u8>> {
            Some(unsafe { (*SkipList::search(self, key)??).clone() })
        }
    }

    fn bench_insert_workload<T: Store + 'static>(
        total_threads: usize,
        store: &mut T,
        bencher: Option<&mut Bencher>,
    ) {
        let to_stop = Arc::new(AtomicBool::new(false));
        let mut handlers = Vec::new();
        let start = std::time::Instant::now();

        // If benchmarking, then the main thread runs in the foreground with
        // fewer runtime checks.
        let first_background_thread_id = if bencher.is_some() { 1 } else { 0 };
        for thread_id in first_background_thread_id..total_threads {
            let mut store = store.clone();
            let to_stop = to_stop.clone();
            let benchmarking = bencher.is_some();
            handlers.push(spawn!(move || {
                let mut i: usize = 0;
                while !to_stop.load(AtomicOrdering::Relaxed) {
                    for _ in 0..1000 {
                        let b0 = i * total_threads + thread_id;
                        let b = b0.to_be_bytes();
                        assert!(!store.insert(&b, &b));
                        if store.search(&b) != Some(b.clone().to_vec()) {
                            to_stop.store(true, AtomicOrdering::SeqCst);
                            std::thread::sleep(std::time::Duration::from_millis(10));
                            panic!("thread_id: {thread_id}, i: {i}, b: {b:?}");
                        }
                        i += 1;
                    }
                    if !benchmarking && thread_id == total_threads - 1 && i > 100_000 {
                        to_stop.store(true, AtomicOrdering::Relaxed);
                        break;
                    }
                }
                debug!(&(), "thread_id: {thread_id}, i: {i}");
            }));
        }

        if let Some(bencher) = bencher {
            let mut i = 0;
            bencher.iter(|| {
                let b: usize = i * total_threads;
                store.insert(&b.to_be_bytes(), &b.to_be_bytes());
                i += 1;
            });
            to_stop.store(true, AtomicOrdering::Relaxed);
        }

        loop {
            std::thread::sleep(std::time::Duration::from_millis(100));
            if handlers.iter().all(|handler| handler.is_finished()) {
                break;
            }
            if start.elapsed().as_secs() > 15 {
                to_stop.store(true, AtomicOrdering::Relaxed);
                std::thread::sleep(std::time::Duration::from_millis(100));
                panic!("timeout");
            }
        }

        for handler in handlers {
            unwrap!(handler.join());
        }
    }

    /// This test is just for comparison to `bench_insert_skip`.
    #[ignore]
    #[bench]
    fn bench_insert_vec(b: &mut Bencher) {
        let mut store = Arc::new(RwLock::new(HashMap::new()));
        bench_insert_workload(4, &mut store, Some(b));
    }

    #[bench]
    fn bench_insert_skip(b: &mut Bencher) {
        let mut store = Arc::new(SkipList::default());
        bench_insert_workload(4, &mut store, Some(b));
    }

    #[test]
    fn test_insert_skip_1() {
        let mut store = Arc::new(SkipList::default());
        bench_insert_workload(4, &mut store, None);
    }

    #[test]
    fn test_insert_skip_2() {
        let mut store = Arc::new(SkipList::default());
        let main_handler = spawn!(move || {
            for b0 in 0_u64..100_000 {
                let b = b0.to_be_bytes();
                assert!(store.search(&b).is_none());
                store.insert(&b, &b);
                let option = store.search(&b);
                assert_eq!(option.unwrap(), b);
            }
        });
        let start = std::time::Instant::now();
        loop {
            // TODO(gs): This typically runs between 3 and 4 seconds on Borden.
            // It should be possible to reduce this time. Investigate possible
            // flakiness.
            if start.elapsed().as_secs() > 10 {
                panic!("timeout");
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
            if main_handler.is_finished() {
                break;
            }
        }
        unwrap!(main_handler.join());
    }

    fn print_nodes(store: &SkipList) {
        debug!(&(), "{store:?}");

        let level = 0;
        let mut x_idx = store.head;
        let mut y_idx = store.get_forward(x_idx, level);

        let mut total_seen = 0;
        while !y_idx.is_end() {
            x_idx = y_idx;
            y_idx = store.get_forward(x_idx, level);
            total_seen += 1;
        }

        debug!(&(), "total_seen: {total_seen}");
    }

    fn ugly_search(store: &SkipList, search_key: &[u8]) -> Option<Option<Vec<u8>>> {
        let mut x_idx = store.head;
        let mut y_idx = NodePtrView(std::ptr::null());

        let mut dbg_string = String::new();

        for level in (0..store.level_hint.read() + 1).rev() {
            y_idx = store.get_forward(x_idx, level);
            dbg_string.push_str(&format!("search-level {level} {y_idx:?}\n"));

            'inner: loop {
                if y_idx.is_end() {
                    dbg_string.push_str("breaking after hitting end of list\n");
                    break 'inner;
                }
                let found_key = y_idx.read_key();
                if found_key >= search_key {
                    let fk = usize::from_be_bytes(unwrap!(found_key.try_into()));
                    dbg_string.push_str(&format!(
                        "breaking because fk {} >= search key {:?}\n",
                        fk,
                        usize::from_be_bytes(unwrap!(search_key.try_into()))
                    ));
                    break 'inner;
                }
                x_idx = y_idx;

                dbg_string.push_str("search-iter\n");
                dbg_string.push_str(&format!("x_idx: {x_idx:?}\n"));
                dbg_string.push_str(&format!(
                    "search_key: {:?}\n",
                    usize::from_be_bytes(search_key.try_into().unwrap())
                ));
                dbg_string.push_str(&format!(
                    "store.read_key(x_idx): {:?}\n",
                    usize::from_be_bytes(x_idx.read_key().try_into().unwrap())
                ));
                let node_level = store.node_level(x_idx);
                //assert_eq!(node_level, level);
                dbg_string.push_str(&format!("store.node_level(x_idx): {node_level}\n"));
                y_idx = store.get_forward(x_idx, level);
                dbg_string.push_str(&format!(
                    "y_idx = store.get_forward(x_idx, level); where\n\ty_idx (next ptr) = \
                     {y_idx:?}\n\tx_idx = {x_idx:?}\n\tlevel = {level}\n"
                ));
                dbg_string.push_str(&format!("y_idx (next ptr): {y_idx:?}\n"));
            }
        }
        //let y_idx = store.get_forward(x_idx, 0);
        dbg_string.push_str(&format!(
            "y_idx = store.get_forward(x_idx, level); where\n\ty_idx (next ptr after loop) = \
             {y_idx:?}\n\tx_idx = {x_idx:?}\n\tlevel = 0\n"
        ));
        dbg_string.push_str(&format!("y_idx (next ptr after loop): {y_idx:?}\n"));
        if !y_idx.is_end() {
            dbg_string.push_str(&format!(
                "store.read_key(y_idx): {:?}\n",
                usize::from_be_bytes(y_idx.read_key().try_into().unwrap())
            ));
        }

        debug_assert!(x_idx == store.head || x_idx.read_key() < search_key);
        if y_idx.is_end() || y_idx.read_key() != search_key {
            print_nodes(store);
            debug!(&(), "{dbg_string}\n^^ugly_search for key {search_key:?}");
            return None;
        }
        Some(y_idx.read_value().cloned())
    }

    #[test]
    fn test_insert_skip_3() {
        let total_threads = 8;
        let top = 64;

        let store = Arc::new(SkipList::default());
        let mut handlers = Vec::new();
        let to_stop = Arc::new(AtomicBool::new(false));
        let all_completed = Arc::new(
            (0..total_threads)
                .map(|_| Arc::new(AtomicUsize::new(0)))
                .collect::<Vec<_>>(),
        );
        let start = std::time::Instant::now();

        for thread_id in 0..total_threads {
            let mut store = store.clone();
            let to_stop = to_stop.clone();
            let all_completed = all_completed.clone();
            let completed = (*all_completed)[thread_id].clone();
            handlers.push(spawn!(move || {
                for i in 0..top {
                    if to_stop.load(AtomicOrdering::Relaxed) {
                        break;
                    }
                    let b0: usize = i * total_threads + thread_id;
                    let b = b0.to_be_bytes();
                    assert!(!store.insert(&b, &b));
                    let search_result = ugly_search(&store, &b);
                    let found: Option<&[u8]> = match search_result.as_ref() {
                        Some(Some(v)) => Some(v.as_slice()),
                        _ => None,
                    };
                    if found != Some(&b) {
                        to_stop.store(true, AtomicOrdering::SeqCst);
                        std::thread::sleep(std::time::Duration::from_millis(10));
                        let thread_progress = (*all_completed)
                            .iter()
                            .map(|arc| (*arc).load(AtomicOrdering::SeqCst))
                            .collect::<Vec<_>>();
                        let total_progress = thread_progress.iter().sum::<usize>();
                        panic!(
                            "thread_id: {thread_id}, i: {i}, b0: {b0}, b: {b:?},\n\ttotal \
                             progress: {total_progress},\n\tthread progress: {thread_progress:?}, \
                             \n\tfound: {found:?}",
                        );
                    }
                    completed.fetch_add(1, AtomicOrdering::Relaxed);
                }
            }));
        }

        loop {
            std::thread::sleep(std::time::Duration::from_millis(100));
            if handlers.iter().all(|handler| handler.is_finished()) {
                break;
            }
            if start.elapsed().as_secs() > 5 {
                panic!("timeout");
            }
        }

        for handler in handlers {
            unwrap!(handler.join());
        }
    }
    #[test]
    fn test_new_set_1() {
        let store = SkipList::default();
        store.insert(&1_u32.to_be_bytes(), Some(&1_u32.to_be_bytes()));
    }

    #[test]
    fn test_new_set_2() {
        let store = SkipList::default();
        store.insert(&1_u32.to_be_bytes(), Some(&1_u32.to_be_bytes()));
        store.insert(&2_u32.to_be_bytes(), Some(&2_u32.to_be_bytes()));
    }

    #[test]
    fn test_new_set_3() {
        let mut store = Arc::new(SkipList::default());
        for b in 0_u64..1000_u64 {
            Store::insert(&mut store, &b.to_be_bytes(), &b.to_be_bytes());
        }
    }

    #[test]
    fn test_new_set_4() {
        let store = SkipList::default();
        store.insert(&1_u32.to_be_bytes(), Some(&1_u32.to_be_bytes()));
        store.insert(&1_u32.to_be_bytes(), Some(&2_u32.to_be_bytes()));
        store.insert(&1_u32.to_be_bytes(), None);
    }

    #[test]
    fn test_xor_shift_randomness() {
        // The XorShift RNG is not cryptographically secure, but it should be
        // good enough for our purposes.

        let seeds = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        for seed in seeds {
            let list = SkipList::default();
            list.xs.store(seed, AtomicOrdering::SeqCst);
            let mut even_count = 0;
            let mut odd_count = 0;
            for _ in 0..1000 {
                let num = list.gen();
                if num % 2 == 0 {
                    even_count += 1;
                } else {
                    odd_count += 1;
                }
            }
            let diff = i32::abs(even_count - odd_count);
            assert!(
                diff < 100,
                "Seed: {seed}, even: {even_count}, odd: {odd_count}"
            );
        }
    }
}
