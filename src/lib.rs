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

    /// Attempt to acquire an [`InflightGuard`] or wait until it is
    /// available otherwise by blocking the current thread.
    pub fn acquire_or_wait(&self, key: &str) -> InflightGuard<'_> {
        loop {
            match self.acquire(key) {
                Ok(guard) => break guard,
                Err(_) => {
                    std::hint::spin_loop();
                    continue;
                }
            };
        }
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
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::{Duration, Instant},
    };

    use crate::{InflightSet, InflightSetError};

    #[test]
    fn key_drop_semantics() {
        let s = InflightSet::new();
        assert_eq!(s.len(), 0, "No keys registered on creation");

        let guard = s.acquire("my_job_id").unwrap();
        assert_eq!(s.len(), 1);
        drop(guard);
        assert_eq!(
            s.len(),
            0,
            "After drop, the key should be removed from the set"
        );
    }

    #[test]
    fn duplicate_key() {
        let s = InflightSet::new();

        let _guard = s.acquire("key_123").unwrap();
        assert_eq!(s.len(), 1);
        let e = s.acquire("key_123").unwrap_err();
        assert!(matches!(e, InflightSetError::DuplicateKey(_)));
        assert!(e.to_string().contains("key_123"));
        assert_eq!(
            s.len(),
            1,
            "Guard rejection should not increase stored keys"
        );
    }

    #[test]
    fn same_key_after_drop() {
        let s = InflightSet::new();
        let name = "test-key";
        let guard = s.acquire(name).expect("unique key, no errors");
        drop(guard);
        assert!(
            s.acquire(name).is_ok(),
            "Valid acquire of {name}, it has been released after drop"
        );
    }

    #[test]
    fn acquire_same_key_many_threads() {
        let s = Arc::new(InflightSet::new());
        let _guard = s.acquire("my-key").expect("unique key succeeds");

        let mut work = Vec::new();
        for _ in 0..1_000 {
            let s = Arc::clone(&s);
            work.push(std::thread::spawn(move || {
                s.acquire("my-key")
                    .expect_err("thread trying to acquire duplicate key should error")
            }));
        }

        for w in work {
            assert!(matches!(
                w.join().unwrap(),
                InflightSetError::DuplicateKey(_)
            ));
        }
    }

    #[test]
    fn acquire_or_wait() {
        let s = Arc::new(InflightSet::new());
        let key = "my-key";

        let guard = s.acquire(key).unwrap();
        let called_acquire_or_wait = Arc::new(AtomicBool::new(false));

        let handle = std::thread::spawn({
            let called_captured = Arc::clone(&called_acquire_or_wait);
            let s_captured = Arc::clone(&s);
            move || {
                s_captured.acquire_or_wait(key);
                called_captured.store(true, Ordering::SeqCst)
            }
        });

        // Sleep the main thread to ensure that the background thread
        // is always blocked for some short period.
        std::thread::sleep(Duration::from_millis(100));

        assert!(
            !called_acquire_or_wait.load(Ordering::SeqCst),
            "acquire_or_wait returned before guard was dropped"
        );

        // Unblock the background thread
        drop(guard);

        let start = Instant::now();
        loop {
            if start.elapsed() >= Duration::from_secs(2) {
                panic!("Background thread was stuck, it should be unblocked from dropped guard");
            }

            if handle.is_finished() {
                break;
            }
        }
        handle.join().unwrap();
        assert!(called_acquire_or_wait.load(Ordering::SeqCst));
    }
}
