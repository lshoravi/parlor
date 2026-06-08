# I/O Readiness Events and Timer Improvements

## Goal

Add socket-level CML events for I/O readiness (`accept-evt`, `readable-evt`, `writable-evt`), fix timer cancellation in choose, and add `timer-operation` (absolute expiry).

## I/O Events

### New module: `src/io.rs`

Three new BaseEvents for socket I/O, exposed as `(cml io)` Scheme library.

### `accept-evt(listener)`

Takes an `Arc<TcpListener>` (created by scheme-rs's `bind-tcp`).

- `try_fn`: attempt non-blocking accept. If a connection is pending, accept it, wrap the `TcpStream` as a Port, return `(port . addr-string)` as a pair Value. If no connection pending, return `None`.
- `block_fn`: spawn a tokio task that does `listener.accept().await`, CAS flag W→S, send `(port . addr-string)`.
- `cancel_fn`: abort the spawned task via `AbortHandle`.

Result value: a Scheme pair `(port . addr-string)`.

### `readable-evt(port)`

Takes a Port wrapping a socket (or any fd-backed stream).

- `try_fn`: call `Port::poll_readable()` (non-blocking). If ready, return the port itself as the value. If not ready, return `None`.
- `block_fn`: spawn a tokio task that calls `Port::wait_readable().await`, CAS flag W→S, send the port.
- `cancel_fn`: abort the spawned task.

Result value: the port unchanged. The user composes with `wrap` to do actual I/O:

```scheme
(let ((data (sync (wrap (readable-evt port)
                        (lambda (p) (get-bytevector-some p))))))
  ...)
```

### `writable-evt(port)`

Symmetric to `readable-evt`. Uses `Port::poll_writable()` / `Port::wait_writable()`.

### Upstream requirements (scheme-rs changes)

The Port type needs readiness methods. Following the same extensibility pattern as `raw_fd_fn`, add optional readiness functions to `IntoPort`:

```rust
pub type PollReadableFn = Box<dyn Fn(&dyn Any) -> bool + Send + Sync>;
pub type WaitReadableFn = Box<dyn Fn(&dyn Any) -> BoxFuture<'_, ()> + Send + Sync>;
// same for writable

trait IntoPort {
    // existing methods...
    fn poll_readable_fn() -> Option<PollReadableFn> { None }
    fn wait_readable_fn() -> Option<WaitReadableFn> { None }
    fn poll_writable_fn() -> Option<PollWritableFn> { None }
    fn wait_writable_fn() -> Option<WaitWritableFn> { None }
}
```

Implement for `tokio::net::TcpStream`:

```rust
impl IntoPort for tokio::net::TcpStream {
    fn poll_readable_fn() -> Option<PollReadableFn> {
        Some(Box::new(|any| {
            let stream = any.downcast_ref::<TcpStream>().unwrap();
            stream.try_read(&mut [0u8; 0]).is_ok()
        }))
    }
    fn wait_readable_fn() -> Option<WaitReadableFn> {
        Some(Box::new(|any| {
            let stream = any.downcast_ref::<TcpStream>().unwrap();
            Box::pin(async { stream.readable().await.unwrap(); })
        }))
    }
    // same for writable
}
```

Add public methods on Port:

```rust
impl Port {
    pub fn poll_readable(&self) -> bool { ... }
    pub async fn wait_readable(&self) { ... }
    pub fn poll_writable(&self) -> bool { ... }
    pub async fn wait_writable(&self) { ... }
}
```

These lock PortData, extract the inner port, call the function pointer.

### Scheme API

```scheme
;; (cml io) library
(accept-evt listener)   → BaseEvent, result: (port . addr-string)
(readable-evt port)     → BaseEvent, result: port
(writable-evt port)     → BaseEvent, result: port
(accept listener)       → (sync (accept-evt listener))
```

### Scheme bridge library

```scheme
;; scheme/cml/io.sls
(library (cml io)
  (export accept-evt readable-evt writable-evt accept)
  (import (rnrs) (cml) (cml io bridge))

  (define (accept-evt listener) (%accept-evt listener))
  (define (readable-evt port) (%readable-evt port))
  (define (writable-evt port) (%writable-evt port))
  (define (accept listener) (sync (accept-evt listener))))
```

### Example: concurrent echo server

```scheme
(import (rnrs) (cml) (cml io) (prefix (async) tokio/))

(define (echo-client port)
  (let loop ()
    (sync (readable-evt port))
    (let ((data (get-bytevector-some port)))
      (unless (eof-object? data)
        (sync (writable-evt port))
        (put-bytevector port data)
        (loop)))))

(define (serve listener)
  (let loop ()
    (let-values ([(port addr) (accept listener)])
      (tokio/spawn (lambda () (echo-client port)))
      (loop))))

(let ((listener (tokio/bind-tcp "0.0.0.0:8080")))
  (serve listener))
```

### Example: accept with timeout

```scheme
(let ((result (sync (choose
                      (wrap (accept-evt listener)
                            (lambda (pair) (cons 'connection pair)))
                      (wrap (sleep-evt 30.0)
                            (lambda (_) 'timeout))))))
  (case (car result)
    ((connection) (handle-client (cadr result)))
    ((timeout) (display "no connection in 30s\n"))))
```

## Timer Improvements

### Timer cancel fix

Currently, timer block_fn spawns a tokio task and drops the JoinHandle. When a timer loses a choose, the task runs to completion (sleeping for potentially 10+ seconds) before being discarded.

Fix: share an `AbortHandle` between block_fn and cancel_fn.

```rust
let abort_slot: Arc<std::sync::Mutex<Option<AbortHandle>>> =
    Arc::new(std::sync::Mutex::new(None));

let slot_for_block = abort_slot.clone();
let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
    let handle = tokio::spawn(async move {
        tokio::time::sleep(duration).await;
        if cas(&flag, OpState::Waiting, OpState::Synched) {
            let _ = tx.send(Value::from(false));
        }
    });
    *slot_for_block.lock().unwrap() = Some(handle.abort_handle());
});

let cancel_fn: CancelFn = Arc::new(move || {
    if let Some(h) = abort_slot.lock().unwrap().take() {
        h.abort();
    }
});
```

This applies to all events that spawn tasks in block_fn: timers, conditions, notifiers, custom events, and the new I/O events.

### `timer-operation` (absolute expiry)

```scheme
(timer-operation expiry)  → BaseEvent
```

Takes an absolute time value (in seconds since epoch, as an inexact number). Computes `duration = max(0, expiry - current-time)` and delegates to the same sleep logic.

```rust
#[bridge(name = "%timer-operation", lib = "(cml timers bridge)")]
pub async fn timer_operation(expiry: f64) -> Result<Vec<Value>, Exception> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    let remaining = (expiry - now).max(0.0);
    // same as sleep_evt with remaining as duration
    ...
}
```

Scheme API addition to `(cml timers)`:

```scheme
(timer-operation expiry)  → BaseEvent, succeeds when current time >= expiry
```

## File Changes

| File | Change |
|---|---|
| `src/io.rs` | **New.** accept-evt, readable-evt, writable-evt BaseEvents + bridges. |
| `src/timers.rs` | **Modify.** Add AbortHandle cancel_fn. Add timer-operation bridge. |
| `src/conditions.rs` | **Modify.** Add AbortHandle cancel_fn to condition/notifier block_fns. |
| `src/custom.rs` | **Modify.** Add AbortHandle cancel_fn to custom event block_fn. |
| `src/lib.rs` | **Modify.** Add `pub mod io`. |
| `scheme/cml/io.sls` | **New.** Scheme library for I/O events. |
| `scheme/cml/timers.sls` | **Modify.** Add timer-operation export. |
| `tests/cml_io.scm` | **New.** Tests for accept-evt, readable-evt, writable-evt. |
| `tests/integration.rs` | **Modify.** Add test entry for cml_io.scm. |

### Upstream (scheme-rs) changes needed

| File | Change |
|---|---|
| `src/ports.rs` | Add `poll_readable_fn`, `wait_readable_fn`, `poll_writable_fn`, `wait_writable_fn` to `IntoPort` trait. Add implementations for `TcpStream`. Add `Port::poll_readable()`, `Port::wait_readable()`, `Port::poll_writable()`, `Port::wait_writable()` public methods. Store function pointers in `BinaryPortData`. |

## Success Criteria

1. `accept-evt` works with `choose` (accept with timeout pattern).
2. `readable-evt` / `writable-evt` work with `choose` and `wrap`.
3. Losing timers in `choose` are cancelled immediately (no lingering sleep tasks).
4. `timer-operation` with absolute expiry works.
5. All existing tests still pass.
6. New I/O tests pass: echo server, accept with timeout, readable/writable composition.
