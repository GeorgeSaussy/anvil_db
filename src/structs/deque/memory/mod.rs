//! This memory implementation is based on the paper
//! "Efficient and Reliable Lock-Free Memory Reclamation Based on
//! Reference Counting" by A. Gidenstam, M. Papatriantafilou, and H. Sundell
//! and P. Tsigas.
//!
//! https://citeseerx.ist.psu.edu/document?repid=rep1&type=pdf&doi=a4a26e1d3231ab823ff13eda49cc01cc4aedc16c

mod bac_interface;
pub(crate) mod bac_shared;
pub(crate) mod cleanup;
pub(crate) mod memory_interface;
pub(crate) mod node;

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicPtr, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::spawn;

    use crate::structs::deque::memory::bac_shared::SimpleBewareAndCleanup;
    use crate::structs::deque::memory::memory_interface::MemoryInterface;
    use crate::structs::deque::memory::node::{Node, UserProps};

    struct QProps<T> {
        next: AtomicPtr<Node<QProps<T>>>,
        value: Option<T>,
    }

    impl<T: Sized> UserProps for QProps<T> {
        fn links(&self) -> Vec<&AtomicPtr<Node<Self>>> {
            vec![&self.next]
        }
    }

    /// [Queue] is based on the queue implementation in the paper.
    struct Queue<T: 'static> {
        bac: SimpleBewareAndCleanup<QProps<T>>,
        head: AtomicPtr<Node<QProps<T>>>,
        tail: AtomicPtr<Node<QProps<T>>>,
    }

    impl<T: Clone> Queue<T> {
        fn new() -> Self {
            let bac = SimpleBewareAndCleanup::new(5, 1024, 1024);

            let mut bac_view = Box::new(bac.view(0));

            let node = bac_view
                .new_node(QProps {
                    next: AtomicPtr::new(std::ptr::null_mut()),
                    value: None,
                })
                .unwrap();

            let queue = Self {
                bac,
                head: AtomicPtr::new(std::ptr::null_mut()),
                tail: AtomicPtr::new(std::ptr::null_mut()),
            };

            let mut bac_view = Box::new(queue.bac.view(0));
            bac_view.store_ref(&queue.head, node);
            bac_view.store_ref(&queue.tail, node);

            queue
        }

        fn view(&mut self, thread_id: usize) -> QueueInterface<'_, T> {
            let bac_view = Box::new(self.bac.view(thread_id));
            QueueInterface {
                bac_view,
                head: &self.head,
                tail: &self.tail,
            }
        }
    }

    struct QueueInterface<'a, T> {
        bac_view: Box<dyn MemoryInterface<NodeProps = QProps<T>>>,
        head: &'a AtomicPtr<Node<QProps<T>>>,
        tail: &'a AtomicPtr<Node<QProps<T>>>,
    }

    impl<'a, T: Clone> QueueInterface<'a, T> {
        /// In the paper, this returns a pointer to the inner value.
        /// In this implementation, the caller becomes the owner of the value.
        fn dequeue(&mut self) -> Option<T> {
            loop {
                let node_ptr = self.bac_view.deref_link(self.head);
                let node = unsafe { &mut *node_ptr };
                let value_ref = node.value_mut_ref();
                let next_atomic = &value_ref.next;
                // TODO(t/1571): This is not an atomic read in the paper.
                let next_ptr = self.bac_view.deref_link(next_atomic);
                if next_ptr.is_null() {
                    self.bac_view.release_ref(node_ptr);
                    return None;
                }
                if self.bac_view.cas_ref(self.head, node_ptr, next_ptr) {
                    self.bac_view.delete_node(node_ptr);
                    let next = unsafe { &*next_ptr };
                    let value = next.value_ref().value.clone();
                    self.bac_view.release_ref(next_ptr);
                    return value;
                }
                self.bac_view.release_ref(node_ptr);
                self.bac_view.release_ref(next_ptr);
            }
        }

        fn enqueue(&mut self, value: T) {
            let props = QProps {
                next: AtomicPtr::new(std::ptr::null_mut()),
                value: Some(value),
            };
            let node = self.bac_view.new_node(props).unwrap();
            let old = self.bac_view.deref_link(self.tail);
            let mut prev_ptr = old;
            loop {
                let prev = unsafe { &mut *prev_ptr };
                loop {
                    // TODO(t/1577): This was not an atomic read in the paper.
                    if prev.value_ref().next.load(Ordering::SeqCst).is_null() {
                        break;
                    }
                    let prev_2 = self.bac_view.deref_link(&prev.value_ref().next);
                    if !std::ptr::eq(old, prev_ptr) {
                        self.bac_view.release_ref(prev);
                    }
                    prev_ptr = prev_2;
                }
                if self
                    .bac_view
                    .cas_ref(&prev.value_ref().next, std::ptr::null_mut(), node)
                {
                    break;
                }
            }
            self.bac_view.cas_ref(self.tail, old, node);
            if !std::ptr::eq(old, prev_ptr) {
                self.bac_view.release_ref(prev_ptr);
            }
            self.bac_view.release_ref(old);
            self.bac_view.release_ref(node);
        }
    }

    #[test]
    fn test_single_threaded_queue() {
        let mut queue: Queue<usize> = Queue::new();
        let mut view = queue.view(0);
        view.enqueue(1);
        view.enqueue(2);
        view.enqueue(3);
        assert_eq!(view.dequeue(), Some(1));
        assert_eq!(view.dequeue(), Some(2));
        assert_eq!(view.dequeue(), Some(3));
        assert_eq!(view.dequeue(), None);
    }

    #[test]
    fn test_multithreaded_queue() {
        let top = 5;

        let queue: Arc<Mutex<Queue<usize>>> = Arc::new(Mutex::new(Queue::new()));

        let mut handles = Vec::new();
        for thread_id in 0..top {
            let queue = queue.clone();
            let handle = spawn(move || {
                let mut queue = (*queue).lock().unwrap();
                let mut view = queue.view(thread_id);
                for i in 0..top {
                    view.enqueue(thread_id * top + i);
                }

                let mut found = Vec::new();
                for _ in 0..top {
                    match view.dequeue() {
                        Some(value) => found.push(value),
                        None => return Err(()),
                    }
                }
                Ok(found)
            });
            handles.push(handle);
        }

        let mut found = vec![false; top * top];
        for handle in handles {
            let thread_ok = handle.join().unwrap();
            let thread_found = thread_ok.unwrap();
            assert_eq!(thread_found.len(), top);
            for idx in thread_found {
                assert!(idx < top * top);
                assert!(!found[idx]);
                found[idx] = true;
            }
        }
    }
}
