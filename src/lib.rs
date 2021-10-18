#![feature(array_zip)]

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use bytes::Bytes;

pub type Records = HashMap<Bytes, Bytes>;

type MutexGuardResult<'a, T> = Result<MutexGuard<'a, T>, PoisonError<MutexGuard<'a, T>>>;

// A key value store of `<bytes, bytes>` which allows you to store any valid
// string keys and values as bytes.
pub struct Store(Arc<Mutex<Records>>);

impl Store {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    pub fn get(&self, k: Bytes) -> Option<Bytes> {
        self.try_run(&|guard| guard.get(&k).map(|byte| byte.clone()))
    }

    pub fn set(&self, k: Bytes, v: Bytes) {
        self.try_run(&|mut guard| guard.insert(k.to_owned(), v.to_owned()));
    }

    pub fn remove(&self, k: Bytes) {
        self.try_run(&|mut guard| guard.remove(&k));
    }

    // Applies a closure to the Store if a lock is acquired.
    // Used for setters and getters
    fn try_run<V>(&self, callback: &dyn Fn(MutexGuard<Records>) -> Option<V>) -> Option<V> {
        self.acquire().ok().and_then(callback)
    }

    // Attempts to acquire a lock
    fn acquire(&self) -> MutexGuardResult<Records> {
        self.0.lock()
    }
}

impl Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::Store;
    use bytes::Bytes;

    const KEYS: [&str; 5] = ["hello1", "hello2", "hello3", "hello4", "hello5"];
    const VALS: [&str; 5] = ["world1", "world2", "world3", "world4", "world5"];

    #[test]
    fn get_and_set() {
        let store = init_store();

        let results = KEYS.map(|k| store.get(Bytes::from(k)).unwrap());
        let expected =
            [49, 50, 51, 52, 53].map(|n: u8| Bytes::from(vec![119, 111, 114, 108, 100, n]));

        assert_eq!(results, expected);
    }

    #[test]
    fn remove() {
        let store = init_store();

        store.remove(Bytes::from("hello5"));
        store.remove(Bytes::from("hello1"));
        store.remove(Bytes::from("hello4"));

        let results: Vec<Bytes> = KEYS[1..3]
            .iter()
            .map(|k| store.get(Bytes::from(*k)).unwrap())
            .collect();
        let expected = [50, 51].map(|n: u8| Bytes::from(vec![119, 111, 114, 108, 100, n]));

        assert_eq!(results, expected);
    }

    fn init_store() -> Store {
        let store = Store::new();
        KEYS.zip(VALS) // Populate the store
            .iter()
            .for_each(|(k, v)| store.set(Bytes::from(*k), Bytes::from(*v)));
        store
    }
}
