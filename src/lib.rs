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

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use dashmap::DashMap;
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

#[derive(Debug, Error)]
pub enum CountedSetError {
    #[error("{key} has already permitted the maximum number of guards {max}")]
    MaximumGuardsPermitted { key: String, max: u64 },

    #[error("{0} has already been initialised")]
    AlreadyInitialised(String),

    #[error("attempting to acquire a key which has not been initialised {0}")]
    KeyNotInitialised(String),
}

/// A [`CountedInflightSet`] can pass out an [`CountedSetGuard`] to
/// the same key, up to a maximum count.
#[derive(Debug)]
pub struct CountedInflightSet {
    counted_keys: Arc<DashMap<Arc<str>, MaxWithCurrent>>,
}

#[derive(Debug, Clone)]
struct MaxWithCurrent {
    key: Arc<str>,
    max: u64,
    current: Arc<AtomicU64>,
}

impl MaxWithCurrent {
    fn new(key: Arc<str>, max: u64, current: u64) -> Self {
        Self {
            key,
            max,
            current: Arc::new(AtomicU64::new(current)),
        }
    }

    /// Load the number of active guards for the given key.
    #[allow(dead_code)]
    fn current(&self) -> u64 {
        self.current.load(Ordering::SeqCst)
    }

    pub fn inc(&self) -> Result<(), CountedSetError> {
        if self.current.fetch_add(1, Ordering::SeqCst) >= self.max {
            self.dec(); // Remove the add on error
            return Err(CountedSetError::MaximumGuardsPermitted {
                key: self.key.to_string(),
                max: self.max,
            });
        }
        Ok(())
    }

    /// Decrement the current value, returning the new value.
    fn dec(&self) -> u64 {
        self.current.fetch_sub(1, Ordering::SeqCst) - 1
    }
}

#[derive(Debug)]
pub struct CountedSetGuard<'a> {
    inner: &'a CountedInflightSet,
    max_with_current: MaxWithCurrent,
}

impl Drop for CountedSetGuard<'_> {
    fn drop(&mut self) {
        if self.max_with_current.dec() == 0 {
            // There are no references for this key anymore, so it is
            // safe to remove at this point.
            self.inner.counted_keys.remove(&self.max_with_current.key);
        }
    }
}

impl Default for CountedInflightSet {
    fn default() -> Self {
        Self::new()
    }
}

impl CountedInflightSet {
    pub fn new() -> Self {
        Self {
            counted_keys: Arc::new(DashMap::new()),
        }
    }

    /// Initialise a key to be used within the [`CountedInflightSet`].
    ///
    /// A key must be initialise before it can be used, in order to know the maximum
    /// number of guards that be acquired at any time.
    pub fn initialise_key(&self, key: &str, max: u64) -> Result<(), CountedSetError> {
        let key: Arc<str> = key.into();
        if self.counted_keys.contains_key(&key) {
            return Err(CountedSetError::AlreadyInitialised(key.to_string()));
        }

        self.counted_keys
            .insert(Arc::clone(&key), MaxWithCurrent::new(key, max, 0));
        Ok(())
    }

    /// Acquire a [`CountedSetGuard`] for a pre-initialised key, see ([`CountedInflightSet::initialise_key`])
    /// for further details.
    ///
    /// If the maximum number of guards has been reached, an error will be returned.
    pub fn acquire(&self, key: &str) -> Result<CountedSetGuard<'_>, CountedSetError> {
        match self.counted_keys.get(key) {
            Some(k) => {
                k.inc()?;
                Ok(CountedSetGuard {
                    inner: self,
                    max_with_current: k.value().clone(),
                })
            }
            None => Err(CountedSetError::KeyNotInitialised(key.to_string())),
        }
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

    use crate::{CountedInflightSet, InflightSet, InflightSetError};

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

    #[test]
    fn counted_set_keys_must_be_initialised() {
        let c = CountedInflightSet::new();
        let err = c.acquire("my_key").unwrap_err();
        assert!(matches!(err, crate::CountedSetError::KeyNotInitialised(_)));
    }

    #[test]
    fn counted_set_acquire() {
        let c = CountedInflightSet::new();
        let key = "test-key";
        let max_guards = 2;

        c.initialise_key(key, max_guards)
            .expect("unique key for test");

        // Acquire the same key twice, up to `max_guards`
        let guard_one = c.acquire(key).unwrap();
        let guard_two = c.acquire(key).unwrap();

        let err = c.acquire(key).unwrap_err();
        assert!(
            matches!(err, crate::CountedSetError::MaximumGuardsPermitted { .. }),
            "Attempting to acquire a key past {max_guards} guards should error"
        );

        assert_eq!(guard_one.max_with_current.key.as_ref(), key);
        assert_eq!(guard_one.max_with_current.max, max_guards);
        assert_eq!(guard_one.max_with_current.current(), max_guards);
        drop(guard_two);

        assert_eq!(
            guard_one.max_with_current.max, max_guards,
            "Max value should never change"
        );
        assert_eq!(guard_one.max_with_current.current(), max_guards - 1);

        drop(guard_one);
        assert!(
            c.counted_keys.is_empty(),
            "After both keys are dropped, the set should now be empty"
        );
    }
}
