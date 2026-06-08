# Bug: tokio/spawn deadlocks after CML sync operations

## Summary

When a Scheme program calls CML `sync` (which internally does `tokio::spawn` + `oneshot::await` inside a bridge function) multiple times, subsequent calls to scheme-rs's `(spawn thunk)` from `(async)` deadlock. The spawned tokio task never gets scheduled.

## Minimal reproduction

```scheme
(import (rnrs) (cml) (cml channels) (cml timers) (prefix (async) tokio/))

;; This works fine:
(sync (sleep-evt 0.001))
(display "first sync done\n")

;; This hangs forever — the spawned task never runs:
(let ((ch (make-channel 1)))
  (tokio/spawn (lambda () (send ch 'x)))
  (recv ch))
```

The test harness creates a `tokio::runtime::Runtime::new()` (multi-threaded) and calls `block_on` to run the Scheme program.

## What works

- `(tokio/sleep 0)` followed by `(tokio/spawn ...)` — works
- `(sleep 0.001)` from `(cml timers)` followed by `(tokio/spawn ...)` — works
- Pure scheme-rs `(tokio/sleep)` + `(tokio/spawn)` in any combination — works
- A Rust bridge that does `tokio::spawn` + `oneshot::await` (identical to `perform_base`), called from Scheme, followed by `(tokio/spawn ...)` — works
- CML `sync(sleep-evt)` once followed by `tokio/spawn` in an isolated test file — sometimes works (depends on binary layout)

## What deadlocks

- CML `sync(sleep-evt)` called from a Scheme program with enough preceding code, followed by `(tokio/spawn ...)` — hangs
- The full `cml_api_coverage.scm` test: after ~15 test sections (each calling various CML operations), a final `(tokio/spawn (lambda () (send ch 'x)))` + `(recv ch)` on a buffered channel deadlocks
- The deadlock is in the spawned task never getting scheduled by tokio — not a channel issue

## Key observations

1. **Layout-sensitive**: Adding or removing unrelated Rust functions in the crate shifts the deadlock. A recompilation can make it appear or disappear.

2. **Accumulation-dependent**: The deadlock requires enough preceding CML operations. Running the deadlocking code in isolation (own test file, fewer preceding operations) often works.

3. **Not a channel bug**: The spawned task doesn't even start executing (no output from inside the lambda). The issue is tokio task scheduling, not CML channels.

4. **Library-internal calls work**: `(sleep 0.001)` is defined as `(%sync (sleep-evt (inexact seconds)))` inside `(cml timers)`. This calls the exact same `%sync` bridge with the exact same event. But it doesn't deadlock. Only calls from the test's top-level scope deadlock.

5. **Direct bridge call deadlocks too**: Calling `%sync` directly (not through the `sync` wrapper) from the test also deadlocks. So it's not the extra Scheme function call.

## How CML sync works (the relevant code path)

```rust
// src/event.rs
pub async fn perform_base(event: &BaseEvent) -> Result<Value, Exception> {
    // try_fn: non-blocking check. For sleep-evt with duration > 0, returns None.
    if let Some(value) = (event.try_fn)() {
        return apply_wraps(&event.wrap_fns, value).await;
    }

    // block path: create flag + oneshot, call block_fn, await oneshot
    let flag = new_flag();
    let (tx, rx) = oneshot::channel();
    (event.block_fn)(flag, tx);  // <-- this spawns a tokio task
    let value = rx.await?;       // <-- this awaits the oneshot
    apply_wraps(&event.wrap_fns, value).await
}
```

The timer's `block_fn`:
```rust
let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
    tokio::spawn(async move {           // spawn a detached tokio task
        tokio::time::sleep(duration).await;
        if cas(&flag, OpState::Waiting, OpState::Synched) {
            let _ = tx.send(Value::from(false));
        }
    });
    // JoinHandle is dropped — task is detached
});
```

`block_fn` is synchronous (`Fn`, not async). It calls `tokio::spawn` to create a task, drops the JoinHandle (detaching the task), and returns. `perform_base` then awaits the oneshot receiver. The detached task runs, sleeps, sends on the oneshot, and completes.

## Hypothesis

The detached tokio tasks spawned by `block_fn` accumulate in the runtime. After enough of them (even though they've completed), something in the interaction between:
- scheme-rs's JIT-compiled async code running inside `block_on`
- The tokio multi-threaded runtime's task scheduler
- Detached completed tasks that were never joined

...prevents new `tokio::task::spawn` calls (from scheme-rs's `(spawn thunk)` bridge) from being scheduled.

This may be related to how scheme-rs's `block_on` drives the reactor — if the main task is polling the oneshot receiver, it might not be yielding to the runtime in a way that allows worker threads to pick up new tasks. The fact that it works from within library code but not from top-level suggests a difference in how scheme-rs compiles/executes top-level forms vs library-internal calls.

## Environment

- scheme-rs: lshoravi/scheme-rs fork, branch main (commit 50c8fca)
- tokio 1.x with "full" features
- macOS, Apple Silicon (ARM64)
- scheme-rs-cml with PCML operation protocol (arc-swap, imbl)

## Impact

Tests that mix CML operations with `(async)` `spawn` deadlock. Workaround: use CML's own channel operations (which go through `perform_base` entirely in Rust) instead of `tokio/spawn` for concurrency, or isolate `tokio/spawn` usage into separate test files with minimal preceding CML operations.
