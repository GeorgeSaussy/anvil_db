// The previous plan was to use "Bolt" [1], but the pseudo-code in the thesis
// is full of bugs and there is no published source code.
//
// 1. https://dspace.mit.edu/bitstream/handle/1721.1/130693/1251799942-MIT.pdf
#[allow(dead_code)]
pub(crate) struct ConcurrentHashMap<K, V> {
    foo: Option<K>,
    bar: Option<V>,
}

impl<K, V> ConcurrentHashMap<K, V> {
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        ConcurrentHashMap {
            foo: None,
            bar: None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn get(&self, _key: &K) -> Option<&V> {
        todo!()
    }

    #[allow(dead_code)]
    pub(crate) fn put(&mut self, _key: K, _value: V) {
        todo!()
    }

    #[allow(dead_code)]
    pub(crate) fn delete(&mut self, _key: &K) {
        todo!()
    }
}
