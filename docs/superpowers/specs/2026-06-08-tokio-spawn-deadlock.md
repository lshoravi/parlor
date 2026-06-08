# Bug: GC crash / tokio spawn deadlock after CML operations

## Summary

Two related symptoms when running CML operations in scheme-rs:

1. **GC crash (SIGSEGV/SIGTRAP)**: after enough CML operations (especially `choose` with timers), the GC collector crashes in `visit_children` reading a corrupted vtable pointer. Repro rate ~30% in isolation, ~100% when running 15+ tests sequentially.

2. **Deadlock**: `tokio/spawn` from scheme-rs's `(async)` library stops scheduling tasks after CML `sync` operations. The spawned task never runs.

Both issues are accumulation-sensitive: they require enough preceding CML operations to trigger. Each test passes individually.

## Minimal reproduction (GC crash)

```scheme
(import (rnrs) (cml) (cml channels) (cml timers))

;; 1000 iterations of choose where recv always wins via try-path
(let ((ch (make-channel 1)))
  (do ((iter 0 (+ iter 1)))
      ((= iter 1000))
    (send ch iter)
    (sync (choose (recv-evt ch)
                  (wrap (sleep-evt 10.0) (lambda (_) 'timeout))))))
```

Run: `cargo test --test integration test_cml_stress_choose -- --test-threads=1`
Repeat 10+ times. Crashes ~30% of runs in isolation, ~100% when preceded by other tests.

Crash site: `gc::collection::HeapObject::visit_children` at `collection.rs:248` — reads `self.header.vtable.visit_children` and gets a bad pointer.

## Minimal reproduction (deadlock)

```scheme
(import (rnrs) (cml) (cml timers) (cml channels) (prefix (async) tokio/))

(sync (sleep-evt 0.001))  ;; one CML sync is enough in some layouts

(let ((ch (make-channel 1)))
  (tokio/spawn (lambda () (send ch 'x)))  ;; task never runs
  (recv ch))                               ;; hangs forever
```

This deadlocks when run after enough preceding CML operations (other tests, or multiple syncs in the same file). In isolation it sometimes passes depending on binary layout.

## What we tested

| Pattern | Result |
|---|---|
| `tokio/sleep` × N, then `tokio/spawn` | Works |
| `(sleep 0.001)` from `(cml timers)`, then `tokio/spawn` | Works |
| `(sync (sleep-evt 0.001))` from test top-level, then `tokio/spawn` | Deadlocks |
| `(%sync (sleep-evt 0.001))` direct bridge call, then `tokio/spawn` | Deadlocks |
| Rust bridge doing identical `tokio::spawn` + `oneshot::await`, then `tokio/spawn` | Works |
| Pure scheme-rs `tokio/sleep` × 100 + `tokio/spawn` | Works |
| Many custom events (`make-custom-event`) then `tokio/spawn` | Works |
| 15 tests sequentially (any order) | GC crash ~100% |
| Each test individually | Pass |

## Key observations

1. **Cannot reproduce without CML.** Pure scheme-rs async primitives (`tokio/sleep`, `tokio/spawn`, `tokio/await`) work correctly in any combination. The bug requires CML's `sync`/`perform_base` code path.

2. **Library-internal calls work.** `(sleep 0.001)` is defined as `(%sync (sleep-evt (inexact seconds)))` inside `(cml timers)`. This calls the exact same `%sync` bridge. But it doesn't deadlock. Only calls through `sync` from the top-level program deadlock.

3. **Identical Rust code works as a direct bridge.** We wrote `%perform-like-test` — a bridge function with the exact same body as `perform_base` (call try_fn, create flag+oneshot, call block_fn, await rx). Called from Scheme, it works fine. But `%sync` calling `perform_base` deadlocks.

4. **Layout-sensitive.** Adding or removing unrelated Rust functions shifts the deadlock. Recompilation can make it appear or disappear. This is the classic signature of a memory corruption / use-after-free.

5. **GC crash and deadlock co-occur.** The same test suite that crashes with SIGSEGV under the old `select_all` implementation hangs under the new PCML implementation. Different symptoms, likely same root cause.

## How CML sync works

```rust
// Bridge: %sync
pub async fn sync_bridge(evt_val: &Value) -> Result<Vec<Value>, Exception> {
    let event = evt_val.try_to_rust_type::<BaseEvent>()?;
    let result = perform_base(&event).await?;
    Ok(vec![result])
}

// Core: perform_base
pub async fn perform_base(event: &BaseEvent) -> Result<Value, Exception> {
    if let Some(value) = (event.try_fn)() {
        return apply_wraps(&event.wrap_fns, value).await;
    }
    let flag = new_flag();
    let (tx, rx) = oneshot::channel();
    (event.block_fn)(flag, tx);  // sync call that may tokio::spawn internally
    let value = rx.await?;       // await the oneshot
    apply_wraps(&event.wrap_fns, value).await
}
```

Timer's `block_fn` (called from `perform_base`):
```rust
let block_fn = Arc::new(move |flag: Flag, tx: ResumeTx| {
    tokio::spawn(async move {       // spawns detached tokio task
        tokio::time::sleep(d).await;
        if cas(&flag, W, S) { let _ = tx.send(value); }
    });
    // JoinHandle dropped — task is detached
});
```

## What we ruled out

- **Detached JoinHandle accumulation**: tested explicitly — a bridge doing the same `tokio::spawn` (dropping JoinHandle) + `oneshot::await` pattern works fine when called directly.
- **Gc ref count churn from cloning**: the new PCML implementation eliminated Event cloning. The deadlock persists. The GC crash also persists.
- **Multiple Runtime instances**: the bug reproduces with a single `Runtime::new()` and `--test-threads=1`.
- **Channel message loss**: the new lock-free channels with double-claim protocol are correct. The deadlock occurs even with simple timer events (no channels involved in the failing `sync` call).

## What we haven't ruled out

- **Interaction between scheme-rs's JIT compilation and the tokio runtime**: the fact that library-internal calls work but top-level calls don't suggests the JIT compiles them differently. Top-level forms may be compiled/executed in a way that doesn't properly cooperate with the tokio runtime's task scheduling.
- **GC collector thread interfering with tokio worker threads**: the GC runs on its own thread and may be contending with tokio. The crash in `visit_children` (corrupted vtable) is consistent with use-after-free from GC/runtime interaction.
- **Scheme continuation/barrier state corruption**: `perform_base` is called through a chain of Scheme procedure calls. If the `ContBarrier` state or continuation handling is corrupted by the preceding operations, it could affect how the async bridge returns control to the tokio runtime.

## Environment

- scheme-rs: lshoravi/scheme-rs fork, branch main (commit 50c8fca)
- tokio 1.x with "full" features
- macOS, Apple Silicon (ARM64)
- scheme-rs-cml at commit d8cb924 (PCML rewrite)
