# CML Redesign: PCML Operation Protocol

## Problem

The current implementation bolts CML semantics onto `futures::future::select_all`. Events are cloned into owned futures, losers are dropped, and the interaction with scheme-rs's GC causes intermittent segfaults. Three structural bugs trace back to this:

1. **GC crash**: `select_all` drops losing futures containing Gc-managed values (`Procedure`, `Value`). The ref count churn during high-frequency `choose` loops triggers a use-after-free in the GC collector's `visit_children`. Repro rate ~10-30% on `cml_stress_choose.scm`.
2. **Message loss in choose**: when `select_all` races multiple ready `ChannelRecv` futures, losing futures' already-consumed messages are dropped.
3. **Cloning overhead**: the `Event` enum stores children inline (`Box<Event>`, `Vec<Event>`), forcing deep clones through `collect_leaves`, guard resolution, and `perform_owned`. Each clone duplicates `Procedure` and `Value` Gc handles.

## Solution

Replace the `select_all`-based implementation with the Parallel CML (PCML) operation protocol from the 2009 ICFP paper by Reppy, Russo, and Xiao. This is the same protocol used by guile-fibers.

## Design

### Event Types

Two types:

**`BaseEvent`** — a struct with four function slots:

- `try_fn() -> Option<ThunkFn>`: non-blocking attempt. Returns `Some(thunk)` if immediately ready (the thunk produces the value), `None` otherwise.
- `block_fn(flag, resume_tx)`: registers for wakeup and actively tries to match with a peer. `flag` is a shared `Arc<AtomicU8>` (W/C/S states). `resume_tx` is a callback/channel to deliver the result.
- `cancel_fn()`: cleanup for losing alternatives. May be no-op (channels use lazy GC instead of active cancellation).
- `wrap_fn: Option<Procedure>`: transforms the result after sync. `None` means identity. Composed by `wrap-operation`.

**`ChoiceEvent`** — a flat `Vec<BaseEvent>`. The `choice` constructor eagerly flattens nested choices: a choice of choices becomes one flat vector of base events.

`wrap` does not create a separate type. It returns a new `BaseEvent` with the same try/block/cancel functions but a composed `wrap_fn`. A wrapped channel recv is still a `BaseEvent`.

`guard-evt` is resolved at `perform-operation` time (call the thunk, get an event, proceed). It is not an event type.

### W/C/S Flag Protocol

A shared `Arc<AtomicU8>` with three states:

- **W** (waiting): initial state. The operation hasn't succeeded yet.
- **C** (claimed): an operation is in the process of committing (used in the double-claim protocol for channels).
- **S** (synched): the operation has successfully completed.

For single events, the flag transitions `W→S` when the operation succeeds. For choices, one shared flag covers all alternatives. The first alternative to `CAS(W→C)` or `CAS(W→S)` wins; others see the flag is no longer `W` and back off.

### `perform-operation` (sync)

**For a BaseEvent:**

1. Call `try_fn()`. If `Some(thunk)`, call thunk, apply `wrap_fn`, return.
2. Create flag (AtomicU8, initial `W`). Create `tokio::sync::oneshot` channel.
3. Call `block_fn(flag, resume_tx)`.
4. Await `resume_rx`. On wakeup, apply `wrap_fn`, return.

**For a ChoiceEvent:**

1. **Try path**: iterate base events in random order. Call each `try_fn()`. First `Some(thunk)` wins — call thunk, apply that event's `wrap_fn`, return.
2. **Block path**: create one shared flag. Create a shared result slot (`Arc<Mutex<Option<(usize, Value)>>>`) and a `tokio::sync::Notify`. For each base event at index `i`, create a per-alternative `oneshot` channel and call `block_fn(flag, resume_tx_i)`. Each alternative's resume_tx_i, when sent to, writes `(i, value)` to the shared result slot, notifies, and cancels the other alternatives.
3. Await `notify.notified()`. Read the result slot to get `(winner_index, value)`. Return `events[winner_index].wrap_fn(value)`.

The per-alternative oneshot channels bridge individual block_fn callbacks into the shared notification mechanism. Each block_fn sees a normal oneshot sender — it doesn't know it's part of a choice. The glue layer (created by perform-operation) handles the coordination.

**No `select_all`, no `BoxFuture`, no future dropping.** The only await point is the oneshot receiver. Block functions register waiters and may complete rendezvous inline. No Gc-managed values are moved into futures.

**For guard-evt at sync time:** call the thunk to produce an event, then perform-operation on the result. If the result is a ChoiceEvent, flatten it. If it's a BaseEvent, run it directly.

### Channels

A `CmlChannel` has:

- `putq: ArcSwap<imbl::Vector<SendWaiter>>` — blocked senders
- `getq: ArcSwap<imbl::Vector<RecvWaiter>>` — blocked receivers
- `buffer: Option<ArcSwap<imbl::Vector<Value>>>` — for buffered channels only

`SendWaiter`: `(flag: Arc<AtomicU8>, resume_tx: oneshot::Sender<Value>, message: Value)`
`RecvWaiter`: `(flag: Arc<AtomicU8>, resume_tx: oneshot::Sender<Value>)`

The `resume_tx` is how the matching peer delivers the result back to the blocked `perform-operation` call, which is awaiting the corresponding `resume_rx`.

Both queues are lock-free following guile-fibers' pattern: persistent immutable vectors in atomic-swap containers. All mutations work by load-snapshot → modify → CAS. On CAS failure, retry.

**`recv-evt` try_fn:**
For buffered channels, check the buffer first — if non-empty, pop and return.
Otherwise, load putq snapshot. Scan for a sender whose flag is still `W`. CAS sender's flag `W→S`. If CAS succeeds, take the message, call sender's resume_tx, return `Some(thunk)`. If CAS fails, skip, try next. If no valid sender, return `None`.

**`recv-evt` block_fn(flag, resume_tx):**

1. Enqueue self as `RecvWaiter(flag, resume_tx)` in getq (CAS loop on ArcSwap).
2. Lazily GC stale entries from getq (where `flag == S`) using a decrement counter (same as guile-fibers).
3. Scan putq for a matching sender — the double-claim protocol:
   - `CAS(own_flag, W→C)` — claim ourselves
   - If own CAS fails: another alternative in our choice already won. Stop.
   - `CAS(sender_flag, W→S)` — synch the sender
   - If sender CAS succeeds: set `own_flag = S`, take message, call both resume_txs. Done.
   - If sender CAS fails (sender was `C` — spinning): set `own_flag = W`, spin/retry.
   - If sender CAS fails (sender is `S` — already synched): set `own_flag = W`, skip, try next sender.

`send-evt` is symmetric.

**`recv-evt` cancel_fn:** No-op. Stale waiters (flag == S) are lazily GC'd from the queue. This matches guile-fibers' approach.

**Buffered channels:** An extension beyond guile-fibers. The buffer is an `ArcSwap<imbl::Vector<Value>>`. `try_fn` for recv pops from the buffer. `try_fn` for send pushes to the buffer if not full. When the buffer can't help, fall back to the waiter queue protocol. Buffer operations use the same CAS-on-persistent-vector pattern.

### Timer Events

**`sleep-evt(seconds)` try_fn:** if duration is zero, return `Some(|| #f)`.

**`sleep-evt` block_fn(flag, resume_tx):** spawn a tokio task:
```
tokio::time::sleep(duration).await;
if flag.CAS(W→S) {
    resume_tx.send(#f);
}
```

**`sleep-evt` cancel_fn:** abort the spawned task via `AbortHandle`.

### Condition Events

**`wait-evt(cv)` try_fn:** if `signalled.load()` is true, return `Some(|| #t)`.

**`wait-evt` block_fn:** spawn a tokio task:
```
notify.notified().await;
if flag.CAS(W→S) {
    resume_tx.send(#t);
}
```

**`wait-evt` cancel_fn:** abort the spawned task.

### Notifier Events

**`notify-evt(n)` try_fn:** `sem.try_acquire()`. If Ok, forget the permit, return `Some(|| #t)`.

**`notify-evt` block_fn:** spawn a tokio task:
```
sem.acquire().await;
if flag.CAS(W→S) {
    resume_tx.send(#t);
} else {
    sem.add_permits(1);  // put the permit back — we lost the choice
}
```

**`notify-evt` cancel_fn:** abort the spawned task. If the task acquired a permit before abort, add it back.

### Custom Events

**`make-custom-event(thunk)` try_fn:** call the thunk, return `Some(result_thunk)`. Custom events are always immediately ready.

**`make-custom-event` block_fn:** call try_fn and resume immediately. Custom events never truly block.

**`make-custom-event` cancel_fn:** no-op.

### Task Management

Removed. Users import `(async)` from scheme-rs for task spawning:

```scheme
(import (async) (cml) (cml channels))
(define ch (make-channel))
(spawn (lambda () (send ch 42)))
(recv ch)
```

scheme-rs's `(spawn thunk)` creates a tokio task and returns an awaitable future. `(await future)` collects the result. This is orthogonal to CML — CML handles synchronization, tokio handles scheduling.

`run-tasks`, `spawn-task`, and `yield` are removed from the `(cml)` library.

### Scheme API

```scheme
;; (cml) library
(sync event)                → perform-operation
(choose event ...)          → choice constructor (flattens nested choices)
(wrap event f)              → composes wrap_fn into a new BaseEvent
(guard-evt thunk)           → resolved at sync time
(make-custom-event thunk)   → always-ready BaseEvent

;; (cml channels) library
(make-channel)              → rendezvous channel
(make-channel capacity)     → buffered channel
(send-evt ch msg)           → BaseEvent (put-operation)
(recv-evt ch)               → BaseEvent (get-operation)
(send ch msg)               → (sync (send-evt ch msg))
(recv ch)                   → (sync (recv-evt ch))

;; (cml timers) library
(sleep-evt seconds)         → BaseEvent (timer)
(sleep seconds)             → (sync (sleep-evt seconds))

;; (cml conditions) library
(make-condition)            → condition
(signal! cv)                → signal the condition
(wait-evt cv)               → BaseEvent
(wait cv)                   → (sync (wait-evt cv))
(make-notifier)             → notifier
(notify! n)                 → add a permit
(notify-evt n)              → BaseEvent
```

### Rust API

`CmlProducer` and `CmlConsumer` remain for Rust-side channel access. Their implementation changes to use the new channel internals but the public API is unchanged.

### Dependencies

**Add:**
- `arc-swap` — atomic pointer swaps for lock-free queues
- `imbl` — persistent immutable vectors with O(1) clone

**Remove:**
- `futures` — no more `select_all` / `BoxFuture`

**Keep:**
- `rand` — used for random try-path ordering in choose

**Keep:**
- `scheme-rs` (git, fork)
- `tokio`

### File Changes

| File | Change |
|---|---|
| `src/event.rs` | **Rewrite.** `BaseEvent` struct + `ChoiceEvent`. Protocol functions. `perform_operation`. No `select_all`, no cloning. |
| `src/channels.rs` | **Rewrite.** Lock-free waiter queues with `ArcSwap<imbl::Vector<Waiter>>`. Double-claim protocol. |
| `src/timers.rs` | **Minor.** Returns `BaseEvent` instead of `Event::Timer`. |
| `src/conditions.rs` | **Minor.** Returns `BaseEvent` instead of `Event` variants. |
| `src/custom.rs` | **Minor.** Returns `BaseEvent`. |
| `src/tasks.rs` | **Remove.** |
| `src/producer.rs` | **Adapt.** Same public API, new channel internals. |
| `src/lib.rs` | Remove `tasks` module. |
| `scheme/cml.sls` | Remove `spawn-task`, `run-tasks`, `yield` exports. Remove `(cml tasks bridge)` import. |
| `scheme/cml/channels.sls` | Unchanged. |
| `scheme/cml/conditions.sls` | Unchanged. |
| `scheme/cml/timers.sls` | Unchanged. |
| `tests/*.scm` | Update tests that use `run-tasks`/`spawn-task` to use `(async)` spawn/await. |
| `tests/integration.rs` | Remove task-related Rust tests. |
| `Cargo.toml` | Add `arc-swap`, `imbl`. Remove `futures`, `rand`. |

### Success Criteria

1. All existing non-task tests pass (basic, channels, compose, conditions, integration, api-coverage).
2. Stress tests pass — including `cml_stress_choose` which currently causes GC crashes.
3. `cargo test` passes reliably with default parallel test threads (no `--test-threads=1` needed).
4. No `select_all`, no `BoxFuture`, no `perform_owned` in the codebase.
5. Task-based stress tests rewritten to use `(async)` spawn/await and pass.
