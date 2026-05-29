# inflight-set

Acquire many distinct guards based on a keyed name.

This means that many RAII guards can be given out for _different_ keys, but only a single instance for the given key can be inflight at once.

For example

```rust
let s = InflightSet::new();
let guard = s.acquire("job_id_123").expect("known unique key");
let guard_two = s.acquire("job_id_567").expect("known unique key");

// do things

// Would error! Attempting to acquire guard for a pre-existing key.
let another_guard = s.acquire("job_id_123")?;

drop(guard); // key=job_id_123

let guard = s.acquire("job_id_123").expect("RAII guard dropped, this is okay");
// When guards go out of scope, the key is released freeing it for later use.
```
