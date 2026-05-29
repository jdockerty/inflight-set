//!Acquire many distinct guards based on a keyed name.
//!
//!This means that many RAII guards can be given out for _different_ keys, but only a single instance for the given key can be inflight at once.
//!
//!For example
//!
//!```ignore
//!let s = InflightSet::new();
//!let guard = s.acquire("job_id_123").expect("known unique key");
//!let guard_two = s.acquire("job_id_567").expect("known unique key");
//!
//!// do things
//!
//!// Would error! Attempting to acquire guard for a pre-existing key.
//!let another_guard = s.acquire("job_id_123")?;
//!
//!drop(guard); // key=job_id_123
//!
//!let guard = s.acquire("job_id_123").expect("RAII guard dropped, this is okay");
//!// When guards go out of scope, the key is released freeing it for later use.
//!```

use std::{collections::HashSet, sync::Arc};

use parking_lot::Mutex;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum InflightSetError {
    #[error("guard for {0} has already been given out")]
    DuplicateKey(String),
}

/// RAII guard for the [`InflightSet`].
///
/// When this is dropped, the key is freed from the
/// overarching set.
#[derive(Debug)]
pub struct InflightGuard<'a> {
    inner: &'a InflightSet,
    key: Arc<str>,
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.inner.keys.lock().remove(&self.key);
    }
}

/// A set implementation which allows for many distinct
/// guards, keyed by name, to be given out at one time.
///
/// However, requests to acquire an [`InflightGuard`] when
/// a key is already in use will result in an error. It is
/// a decision on the callee how to handle this.
#[derive(Debug)]
pub struct InflightSet {
    keys: Mutex<HashSet<Arc<str>>>,
}

impl Default for InflightSet {
    fn default() -> Self {
        Self::new()
    }
}

impl InflightSet {
    pub fn new() -> Self {
        Self {
            keys: Mutex::new(HashSet::new()),
        }
    }

    /// Acquire an [`InflightGuard`], assigning a key for the guard acquisition.
    ///
    /// Future [`InflightSet::acquire`] calls which use the same key are rejected until
    /// the guard is dropped.
    pub fn acquire(&self, key: &str) -> Result<InflightGuard<'_>, InflightSetError> {
        let key: Arc<str> = key.into();

        if !self.keys.lock().insert(Arc::clone(&key)) {
            return Err(InflightSetError::DuplicateKey(key.to_string()));
        }

        Ok(InflightGuard { inner: self, key })
    }

    /// The number of active keys, yet to be dropped.
    pub fn len(&self) -> usize {
        self.keys.lock().len()
    }

    /// Whether there are no active keys.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod test {
    use crate::{InflightSet, InflightSetError};

    #[test]
    fn key_drop_semantics() {
        let s = InflightSet::new();
        assert_eq!(s.len(), 0);

        let guard = s.acquire("my_job_id").unwrap();
        assert_eq!(s.len(), 1);
        drop(guard);
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn duplicate_key() {
        let s = InflightSet::new();

        let _guard = s.acquire("key_123").unwrap();
        assert_eq!(s.len(), 1);
        assert!(matches!(
            s.acquire("key_123").unwrap_err(),
            InflightSetError::DuplicateKey(_)
        ));
        assert_eq!(
            s.len(),
            1,
            "Guard rejection should not increase stored keys"
        );
    }
}
