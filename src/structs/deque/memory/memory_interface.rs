use std::sync::atomic::AtomicPtr;

use super::node::Node;

// TODO(t/1571): Delete this interface and directly use `BacInterface` instead.

pub(crate) trait MemoryInterface {
    type NodeProps: Sized;

    fn cas_ref(
        &mut self,
        link: &AtomicPtr<Node<Self::NodeProps>>,
        old: *mut Node<Self::NodeProps>,
        new: *mut Node<Self::NodeProps>,
    ) -> bool;
    fn deref_link(&mut self, link: &AtomicPtr<Node<Self::NodeProps>>)
        -> *mut Node<Self::NodeProps>;
    fn release_ref(&mut self, node: *mut Node<Self::NodeProps>);
    fn store_ref(
        &mut self,
        link: &AtomicPtr<Node<Self::NodeProps>>,
        node: *mut Node<Self::NodeProps>,
    );
    fn new_node(&mut self, user_props: Self::NodeProps) -> Option<*mut Node<Self::NodeProps>>;
    fn delete_node(&mut self, node: *mut Node<Self::NodeProps>);
}
