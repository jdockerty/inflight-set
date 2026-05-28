use std::{collections::HashSet, sync::Arc};

use parking_lot::Mutex;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SemaphoreError {
    #[error("permit for {0} has already been given out")]
    DuplicateKey(String),
}

/// RAII guard for the [`KeyedSemaphore`].
///
/// When this is dropped, the key is freed from the
/// overarching semaphore.
#[derive(Debug)]
pub struct KeyedSemaphorePermit<'a> {
    inner: &'a KeyedSemaphore,
    key: Arc<str>,
}

impl Drop for KeyedSemaphorePermit<'_> {
    fn drop(&mut self) {
        self.inner.keys.lock().remove(&self.key);
    }
}

#[derive(Debug)]
pub struct KeyedSemaphore {
    keys: Mutex<HashSet<Arc<str>>>,
}

impl Default for KeyedSemaphore {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyedSemaphore {
    pub fn new() -> Self {
        Self {
            keys: Mutex::new(HashSet::new()),
        }
    }

    /// Acquire a [`KeyedSemaphorePermit`], assigning a key for the permit acquisition.
    ///
    /// Future [`KeyedSemaphore::acquire`] calls which use the same key are rejected until
    /// the permit is dropped.
    pub fn acquire(&self, key: &str) -> Result<KeyedSemaphorePermit<'_>, SemaphoreError> {
        let key: Arc<str> = key.into();

        if !self.keys.lock().insert(Arc::clone(&key)) {
            return Err(SemaphoreError::DuplicateKey(key.to_string()));
        }

        Ok(KeyedSemaphorePermit { inner: self, key })
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
    use crate::{KeyedSemaphore, SemaphoreError};

    #[test]
    fn key_drop_semantics() {
        let s = KeyedSemaphore::new();
        assert_eq!(s.len(), 0);

        let permit = s.acquire("my_job_id").unwrap();
        assert_eq!(s.len(), 1);
        drop(permit);
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn duplicate_key() {
        let s = KeyedSemaphore::new();

        let _permit = s.acquire("key_123").unwrap();
        assert_eq!(s.len(), 1);
        assert!(matches!(
            s.acquire("key_123").unwrap_err(),
            SemaphoreError::DuplicateKey(_)
        ));
        assert_eq!(
            s.len(),
            1,
            "Permit rejection should not increase stored keys"
        );
    }
}
