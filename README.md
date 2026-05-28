# keyed-semaphore

Acquire many distinct permits based on a keyed name.

This means that many RAII guards can be given out for _different_ keys, but only a single instance for the given key.

For example

```rust
let s = KeyedSemaphore::new();
let permit = s.acquire("job_id_123").expect("known unique key");
let permit_two = s.acquire("job_id_567").expect("known unique key");

// do things

let another_permit = s.acquire("job_id_123")?; // Would error! Attempting to acquire permit for a pre-existing key.
drop(permit);
let permit = s.acquire("job_id_123").expect("RAII guard dropped, this is okay") 
// When permits goes out of scope, they are dropped and the key is released, freeing the key for use.
```
