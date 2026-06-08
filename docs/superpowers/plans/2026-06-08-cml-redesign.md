# CML Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `select_all`-based CML implementation with the PCML operation protocol (W/C/S flag, try/block/cancel), lock-free channels, and tokio-based suspension.

**Architecture:** Two event types (BaseEvent, ChoiceEvent) with four protocol functions each. Channels use lock-free waiter queues (`ArcSwap<imbl::Vector<Waiter>>`). `perform_operation` uses a oneshot channel for suspension instead of racing futures. Task management removed — users use scheme-rs's `(async)` library.

**Tech Stack:** Rust, scheme-rs (with async+tokio features), tokio, arc-swap, imbl

**Spec:** `docs/superpowers/specs/2026-06-08-cml-redesign.md`

---

## File Map

| File | Responsibility | Change |
|---|---|---|
| `Cargo.toml` | Dependencies | Add arc-swap, imbl. Remove futures. |
| `src/lib.rs` | Module declarations | Remove tasks module. |
| `src/event.rs` | BaseEvent, ChoiceEvent, OpState flag, perform_operation, wrap, choose, guard-evt | Full rewrite. |
| `src/channels.rs` | CmlChannel, SendWaiter, RecvWaiter, lock-free queues, put/get operations | Full rewrite. |
| `src/timers.rs` | Timer BaseEvent constructor + bridge | Rewrite to return BaseEvent. |
| `src/conditions.rs` | Condition, Notifier types + BaseEvent constructors + bridges | Rewrite to return BaseEvent. |
| `src/custom.rs` | Custom event BaseEvent constructor + bridge | Rewrite to return BaseEvent. |
| `src/producer.rs` | CmlProducer, CmlConsumer for Rust-side channel access | Adapt to new channel internals. |
| `src/tasks.rs` | (deleted) | Remove. |
| `scheme/cml.sls` | Main CML Scheme library | Remove task exports, update imports. |
| `scheme/cml/channels.sls` | Channel Scheme library | Unchanged. |
| `scheme/cml/conditions.sls` | Conditions Scheme library | Unchanged. |
| `scheme/cml/timers.sls` | Timers Scheme library | Unchanged. |
| `tests/integration.rs` | Rust test harness | Remove task tests, update for new API. |
| `tests/*.scm` | Scheme test files | Update task-based tests to use (async). |

---

### Task 1: Update Dependencies

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: Update Cargo.toml**

Replace the dependencies section:

```toml
[package]
name = "scheme-rs-cml"
version = "0.1.0"
edition = "2024"

[dependencies]
scheme-rs = { git = "ssh://git@github.com/lshoravi/scheme-rs.git", branch = "main", features = ["async", "tokio"] }
tokio = { version = "1", features = ["full"] }
arc-swap = "1"
imbl = "3"
rand = "0.9"

[dev-dependencies]
tokio = { version = "1", features = ["full", "test-util"] }
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check 2>&1 | tail -5`

This will fail because existing code still imports `futures`. That's expected — we're changing dependencies first, implementation next.

- [ ] **Step 3: Commit**

```
git add Cargo.toml Cargo.lock
git commit -m "chore: add arc-swap, imbl; remove futures dependency"
```

---

### Task 2: BaseEvent and OpState Types

**Files:**
- Rewrite: `src/event.rs`

This is the core of the redesign. We define the types, the flag protocol, and `perform_operation`. No bridges yet — just the Rust types and logic.

- [ ] **Step 1: Write the new event.rs**

```rust
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use scheme_rs::exceptions::Exception;
use scheme_rs::gc::{OpaqueGcPtr, Trace};
use scheme_rs::proc::{ContBarrier, Procedure};
use scheme_rs::records::{RecordTypeDescriptor, SchemeCompatible, rtd};
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;
use rand::seq::SliceRandom;
use tokio::sync::{Notify, Mutex, oneshot};

// --- Flag Protocol ---

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpState {
    Waiting = 0,
    Claimed = 1,
    Synched = 2,
}

pub type Flag = Arc<AtomicU8>;

pub fn new_flag() -> Flag {
    Arc::new(AtomicU8::new(OpState::Waiting as u8))
}

pub fn cas(flag: &Flag, expected: OpState, desired: OpState) -> bool {
    flag.compare_exchange(
        expected as u8,
        desired as u8,
        Ordering::SeqCst,
        Ordering::SeqCst,
    )
    .is_ok()
}

pub fn flag_state(flag: &Flag) -> OpState {
    match flag.load(Ordering::SeqCst) {
        0 => OpState::Waiting,
        1 => OpState::Claimed,
        2 => OpState::Synched,
        _ => unreachable!(),
    }
}

// --- Resume mechanism ---

pub type ResumeTx = oneshot::Sender<Value>;
pub type ResumeRx = oneshot::Receiver<Value>;

// --- BaseEvent ---

pub type TryFn = Box<dyn Fn() -> Option<Value> + Send + Sync>;
pub type BlockFn = Box<dyn Fn(Flag, ResumeTx) + Send + Sync>;
pub type CancelFn = Box<dyn Fn() + Send + Sync>;

pub struct BaseEvent {
    pub try_fn: TryFn,
    pub block_fn: BlockFn,
    pub cancel_fn: CancelFn,
    pub wrap_fn: Option<Procedure>,
}

unsafe impl Trace for BaseEvent {
    unsafe fn visit_children(&self, visitor: &mut dyn FnMut(OpaqueGcPtr)) {
        if let Some(ref wrap) = self.wrap_fn {
            unsafe { wrap.visit_children(visitor) };
        }
    }

    unsafe fn finalize(&mut self) {
        unsafe { std::ptr::drop_in_place(self as *mut Self) }
    }
}

impl SchemeCompatible for BaseEvent {
    fn rtd() -> Arc<RecordTypeDescriptor> {
        rtd!(
            name: "cml-event",
            opaque: true,
            sealed: true,
        )
    }
}

// --- ChoiceEvent ---

pub struct ChoiceEvent {
    pub alternatives: Vec<BaseEvent>,
}

unsafe impl Trace for ChoiceEvent {
    unsafe fn visit_children(&self, visitor: &mut dyn FnMut(OpaqueGcPtr)) {
        for alt in &self.alternatives {
            unsafe { alt.visit_children(visitor) };
        }
    }

    unsafe fn finalize(&mut self) {
        unsafe { std::ptr::drop_in_place(self as *mut Self) }
    }
}

impl SchemeCompatible for ChoiceEvent {
    fn rtd() -> Arc<RecordTypeDescriptor> {
        rtd!(
            name: "cml-choice-event",
            opaque: true,
            sealed: true,
        )
    }
}

// --- Event enum for Scheme interop ---

// Both BaseEvent and ChoiceEvent are separate SchemeCompatible types.
// The bridge functions accept a Value and try both.

fn value_to_base_event(val: &Value) -> Result<std::sync::Arc<BaseEvent>, Exception> {
    // try_to_rust_type returns a Gc<T>, we need to work with that
    val.try_to_rust_type::<BaseEvent>()
        .map(|gc| {
            // We can't easily extract from Gc, so we'll rethink this.
            // For now, placeholder — see note below.
            todo!("need to figure out Gc<BaseEvent> access pattern")
        })
}

// --- perform_operation ---

async fn apply_wrap(wrap_fn: &Option<Procedure>, value: Value) -> Result<Value, Exception> {
    match wrap_fn {
        None => Ok(value),
        Some(proc) => {
            let results = proc.call(&[value], &mut ContBarrier::new()).await?;
            results.into_iter().next().ok_or_else(|| {
                Exception::error("wrap function returned no values")
            })
        }
    }
}

pub async fn perform_base(event: &BaseEvent) -> Result<Value, Exception> {
    // Try path
    if let Some(value) = (event.try_fn)() {
        return apply_wrap(&event.wrap_fn, value).await;
    }

    // Block path
    let flag = new_flag();
    let (tx, rx) = oneshot::channel();
    (event.block_fn)(flag, tx);
    let value = rx.await.map_err(|_| Exception::error("operation cancelled"))?;
    apply_wrap(&event.wrap_fn, value).await
}

pub async fn perform_choice(choice: &ChoiceEvent) -> Result<Value, Exception> {
    let alts = &choice.alternatives;
    if alts.is_empty() {
        return Err(Exception::error("choose: no alternatives"));
    }
    if alts.len() == 1 {
        return perform_base(&alts[0]).await;
    }

    // Try path: random order
    let mut indices: Vec<usize> = (0..alts.len()).collect();
    indices.shuffle(&mut rand::rng());
    for &i in &indices {
        if let Some(value) = (alts[i].try_fn)() {
            return apply_wrap(&alts[i].wrap_fn, value).await;
        }
    }

    // Block path: shared flag, per-alternative oneshot bridged to shared notify
    let flag = new_flag();
    let result_slot: Arc<Mutex<Option<(usize, Value)>>> = Arc::new(Mutex::new(None));
    let notify = Arc::new(Notify::new());

    let mut cancel_handles: Vec<Option<tokio::task::AbortHandle>> = Vec::new();

    for (i, alt) in alts.iter().enumerate() {
        let (tx, rx) = oneshot::channel::<Value>();
        let slot = result_slot.clone();
        let notify_clone = notify.clone();
        let flag_clone = flag.clone();

        // Spawn a bridging task: awaits the per-alternative oneshot,
        // writes to the shared slot, and notifies.
        let handle = tokio::spawn(async move {
            if let Ok(value) = rx.await {
                let mut guard = slot.lock().await;
                if guard.is_none() {
                    *guard = Some((i, value));
                }
                notify_clone.notify_one();
            }
        });
        cancel_handles.push(Some(handle.abort_handle()));

        (alt.block_fn)(flag.clone(), tx);
    }

    // Await notification
    notify.notified().await;

    // Read result
    let (winner_index, value) = result_slot
        .lock()
        .await
        .take()
        .ok_or_else(|| Exception::error("choose: no result after notification"))?;

    // Cancel bridging tasks for non-winners
    for (i, handle) in cancel_handles.iter().enumerate() {
        if i != winner_index {
            if let Some(h) = handle {
                h.abort();
            }
            (alts[i].cancel_fn)();
        }
    }

    apply_wrap(&alts[winner_index].wrap_fn, value).await
}

// --- Bridges ---

#[bridge(name = "%sync", lib = "(cml bridge)")]
pub async fn sync_bridge(evt_val: &Value) -> Result<Vec<Value>, Exception> {
    // Try as BaseEvent first, then as ChoiceEvent
    if let Ok(event) = evt_val.try_to_rust_type::<BaseEvent>() {
        let result = perform_base(&event).await?;
        return Ok(vec![result]);
    }
    if let Ok(choice) = evt_val.try_to_rust_type::<ChoiceEvent>() {
        let result = perform_choice(&choice).await?;
        return Ok(vec![result]);
    }
    Err(Exception::error("sync: expected an event"))
}

#[bridge(name = "%wrap", lib = "(cml bridge)")]
pub async fn wrap_bridge(evt_val: &Value, transform: Procedure) -> Result<Vec<Value>, Exception> {
    if let Ok(event) = evt_val.try_to_rust_type::<BaseEvent>() {
        let existing_wrap = event.wrap_fn.clone();
        let new_wrap = match existing_wrap {
            None => transform,
            Some(inner) => {
                // We need to compose: transform(inner(value))
                // For now, store transform; composition happens at apply_wrap time
                // This requires a different approach — store a Vec<Procedure> or compose closures
                // Simplification: store only the outermost wrap, chain at call time
                // TODO: implement proper composition
                transform
            }
        };
        // We can't mutate a Gc<BaseEvent>, so we need to create a new one
        // with the same try/block/cancel but a new wrap_fn.
        // Problem: TryFn/BlockFn/CancelFn are boxed closures — we can't clone them.
        // This is a design issue we need to resolve.
        todo!("BaseEvent closures are not cloneable — need Arc or different design")
    } else {
        Err(Exception::error("wrap: expected an event"))
    }
}

#[bridge(name = "%choose", lib = "(cml bridge)")]
pub async fn choose_bridge(evts: &[Value]) -> Result<Vec<Value>, Exception> {
    let mut alternatives = Vec::new();
    for v in evts {
        if let Ok(event) = v.try_to_rust_type::<BaseEvent>() {
            alternatives.push(event);
        } else if let Ok(choice) = v.try_to_rust_type::<ChoiceEvent>() {
            // Flatten nested choices
            alternatives.extend(choice.alternatives.iter());
        } else {
            return Err(Exception::error("choose: expected events"));
        }
    }
    // Problem: we need to move BaseEvents out of Gc into the Vec.
    // Gc<BaseEvent> doesn't give us ownership.
    todo!("need to resolve Gc<BaseEvent> ownership for ChoiceEvent construction")
}

#[bridge(name = "%guard-evt", lib = "(cml bridge)")]
pub async fn guard_evt_bridge(thunk: Procedure) -> Result<Vec<Value>, Exception> {
    // guard-evt is resolved at sync time, so we return a BaseEvent whose
    // try_fn calls the thunk and syncs on the result.
    // But the thunk is async (it's a Scheme procedure)...
    // try_fn is sync (Fn() -> Option<Value>). We can't call async from try_fn.
    // This means guard-evt can't be a BaseEvent with a try_fn.
    // It needs special handling in perform_operation.
    todo!("guard-evt needs special handling — can't be a BaseEvent")
}
```

**Note:** This first pass reveals several design issues that need resolution before proceeding:

1. `BaseEvent` closures (`TryFn`, `BlockFn`, `CancelFn`) are `Box<dyn Fn>` — not cloneable, so `wrap` can't create a new BaseEvent reusing the same closures.
2. `Gc<BaseEvent>` gives shared access, not ownership — `choose` can't move events into a Vec.
3. `guard-evt` thunk is async but `try_fn` is sync.
4. `wrap` composition needs to chain `Procedure` calls.

**Resolution:** Use `Arc` for the closure fields so they can be shared. Store BaseEvents inside ChoiceEvent by reference (Arc or Gc). Handle guard-evt as a special case in sync_bridge rather than as a BaseEvent. Let me revise.

- [ ] **Step 2: Revise event.rs with corrected types**

The key insight: `BaseEvent` fields should use `Arc<dyn Fn...>` so the event can be cheaply shared (wrap creates a new BaseEvent that shares the same Arc'd closures with a new wrap_fn). `ChoiceEvent` stores `Gc<BaseEvent>` references. `guard-evt` is resolved in `sync_bridge` before dispatching to `perform_base`/`perform_choice`.

Write the corrected `src/event.rs`:

```rust
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use scheme_rs::exceptions::Exception;
use scheme_rs::gc::{Gc, OpaqueGcPtr, Trace};
use scheme_rs::proc::{ContBarrier, Procedure};
use scheme_rs::records::{RecordTypeDescriptor, SchemeCompatible, rtd};
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;
use rand::seq::SliceRandom;
use tokio::sync::{Notify, Mutex, oneshot};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpState {
    Waiting = 0,
    Claimed = 1,
    Synched = 2,
}

pub type Flag = Arc<AtomicU8>;

pub fn new_flag() -> Flag {
    Arc::new(AtomicU8::new(OpState::Waiting as u8))
}

pub fn cas(flag: &Flag, expected: OpState, desired: OpState) -> bool {
    flag.compare_exchange(
        expected as u8,
        desired as u8,
        Ordering::SeqCst,
        Ordering::SeqCst,
    )
    .is_ok()
}

pub fn flag_state(flag: &Flag) -> OpState {
    match flag.load(Ordering::SeqCst) {
        0 => OpState::Waiting,
        1 => OpState::Claimed,
        2 => OpState::Synched,
        _ => unreachable!(),
    }
}

pub type ResumeTx = oneshot::Sender<Value>;

pub type TryFn = Arc<dyn Fn() -> Option<Value> + Send + Sync>;
pub type BlockFn = Arc<dyn Fn(Flag, ResumeTx) + Send + Sync>;
pub type CancelFn = Arc<dyn Fn() + Send + Sync>;

pub struct BaseEvent {
    pub try_fn: TryFn,
    pub block_fn: BlockFn,
    pub cancel_fn: CancelFn,
    pub wrap_fn: Option<Procedure>,
}

unsafe impl Trace for BaseEvent {
    unsafe fn visit_children(&self, visitor: &mut dyn FnMut(OpaqueGcPtr)) {
        if let Some(ref wrap) = self.wrap_fn {
            unsafe { wrap.visit_children(visitor) };
        }
    }

    unsafe fn finalize(&mut self) {
        unsafe { std::ptr::drop_in_place(self as *mut Self) }
    }
}

impl SchemeCompatible for BaseEvent {
    fn rtd() -> Arc<RecordTypeDescriptor> {
        rtd!(
            name: "cml-event",
            opaque: true,
            sealed: true,
        )
    }
}

pub struct ChoiceEvent {
    pub alternatives: Vec<Value>,
}

unsafe impl Trace for ChoiceEvent {
    unsafe fn visit_children(&self, visitor: &mut dyn FnMut(OpaqueGcPtr)) {
        for alt in &self.alternatives {
            unsafe { alt.visit_children(visitor) };
        }
    }

    unsafe fn finalize(&mut self) {
        unsafe { std::ptr::drop_in_place(self as *mut Self) }
    }
}

impl SchemeCompatible for ChoiceEvent {
    fn rtd() -> Arc<RecordTypeDescriptor> {
        rtd!(
            name: "cml-choice-event",
            opaque: true,
            sealed: true,
        )
    }
}

async fn apply_wrap(wrap_fn: &Option<Procedure>, value: Value) -> Result<Value, Exception> {
    match wrap_fn {
        None => Ok(value),
        Some(proc) => {
            let results = proc.call(&[value], &mut ContBarrier::new()).await?;
            results
                .into_iter()
                .next()
                .ok_or_else(|| Exception::error("wrap function returned no values"))
        }
    }
}

fn resolve_event(val: &Value) -> Result<Gc<BaseEvent>, Exception> {
    val.try_to_rust_type::<BaseEvent>()
}

fn resolve_choice(val: &Value) -> Result<Gc<ChoiceEvent>, Exception> {
    val.try_to_rust_type::<ChoiceEvent>()
}

pub async fn perform_base(event: &BaseEvent) -> Result<Value, Exception> {
    if let Some(value) = (event.try_fn)() {
        return apply_wrap(&event.wrap_fn, value).await;
    }

    let flag = new_flag();
    let (tx, rx) = oneshot::channel();
    (event.block_fn)(flag, tx);
    let value = rx.await.map_err(|_| Exception::error("operation cancelled"))?;
    apply_wrap(&event.wrap_fn, value).await
}

pub async fn perform_choice(choice: &ChoiceEvent) -> Result<Value, Exception> {
    let alts: Vec<Gc<BaseEvent>> = choice
        .alternatives
        .iter()
        .map(|v| resolve_event(v))
        .collect::<Result<Vec<_>, _>>()?;

    if alts.is_empty() {
        return Err(Exception::error("choose: no alternatives"));
    }
    if alts.len() == 1 {
        return perform_base(&alts[0]).await;
    }

    // Try path: random order
    let mut indices: Vec<usize> = (0..alts.len()).collect();
    indices.shuffle(&mut rand::rng());
    for &i in &indices {
        if let Some(value) = (alts[i].try_fn)() {
            return apply_wrap(&alts[i].wrap_fn, value).await;
        }
    }

    // Block path
    let flag = new_flag();
    let result_slot: Arc<Mutex<Option<(usize, Value)>>> = Arc::new(Mutex::new(None));
    let notify = Arc::new(Notify::new());

    let mut abort_handles: Vec<tokio::task::AbortHandle> = Vec::new();

    for (i, alt) in alts.iter().enumerate() {
        let (tx, rx) = oneshot::channel::<Value>();
        let slot = result_slot.clone();
        let notify_clone = notify.clone();

        let handle = tokio::spawn(async move {
            if let Ok(value) = rx.await {
                let mut guard = slot.lock().await;
                if guard.is_none() {
                    *guard = Some((i, value));
                }
                notify_clone.notify_one();
            }
        });
        abort_handles.push(handle.abort_handle());

        (alt.block_fn)(flag.clone(), tx);
    }

    notify.notified().await;

    let (winner_index, value) = result_slot
        .lock()
        .await
        .take()
        .ok_or_else(|| Exception::error("choose: no result after notification"))?;

    for (i, handle) in abort_handles.iter().enumerate() {
        if i != winner_index {
            handle.abort();
            (alts[i].cancel_fn)();
        }
    }

    apply_wrap(&alts[winner_index].wrap_fn, value).await
}

// --- Bridges ---

async fn resolve_guard(thunk: &Procedure) -> Result<Value, Exception> {
    let results = thunk.call(&[], &mut ContBarrier::new()).await?;
    results
        .into_iter()
        .next()
        .ok_or_else(|| Exception::error("guard-evt thunk returned no values"))
}

#[bridge(name = "%sync", lib = "(cml bridge)")]
pub async fn sync_bridge(evt_val: &Value) -> Result<Vec<Value>, Exception> {
    if let Ok(event) = evt_val.try_to_rust_type::<BaseEvent>() {
        let result = perform_base(&event).await?;
        return Ok(vec![result]);
    }
    if let Ok(choice) = evt_val.try_to_rust_type::<ChoiceEvent>() {
        let result = perform_choice(&choice).await?;
        return Ok(vec![result]);
    }
    Err(Exception::error("sync: expected an event"))
}

#[bridge(name = "%wrap", lib = "(cml bridge)")]
pub async fn wrap_bridge(evt_val: &Value, transform: Procedure) -> Result<Vec<Value>, Exception> {
    if let Ok(event) = evt_val.try_to_rust_type::<BaseEvent>() {
        let new_wrap = match &event.wrap_fn {
            None => Some(transform),
            Some(inner) => {
                let inner = inner.clone();
                Some(transform)
                // TODO: proper composition — for now store outermost only.
                // Proper fix: create a composed Procedure or use a Vec<Procedure>.
                // This is addressed in step 3.
            }
        };
        let wrapped = BaseEvent {
            try_fn: event.try_fn.clone(),
            block_fn: event.block_fn.clone(),
            cancel_fn: event.cancel_fn.clone(),
            wrap_fn: new_wrap,
        };
        Ok(vec![Value::from_rust_type(wrapped)])
    } else if let Ok(choice) = evt_val.try_to_rust_type::<ChoiceEvent>() {
        let wrapped_alts: Vec<Value> = choice
            .alternatives
            .iter()
            .map(|v| {
                let mut results = wrap_bridge(v, transform.clone());
                // This is async — we need to handle it differently
                todo!("wrap over choice needs to recursively wrap each alt")
            })
            .collect();
        todo!()
    } else {
        Err(Exception::error("wrap: expected an event"))
    }
}

#[bridge(name = "%choose", lib = "(cml bridge)")]
pub async fn choose_bridge(evts: &[Value]) -> Result<Vec<Value>, Exception> {
    let mut alternatives: Vec<Value> = Vec::new();
    for v in evts {
        if let Ok(_) = v.try_to_rust_type::<BaseEvent>() {
            alternatives.push(v.clone());
        } else if let Ok(choice) = v.try_to_rust_type::<ChoiceEvent>() {
            alternatives.extend(choice.alternatives.iter().cloned());
        } else {
            return Err(Exception::error("choose: expected events"));
        }
    }
    let choice = ChoiceEvent { alternatives };
    Ok(vec![Value::from_rust_type(choice)])
}

#[bridge(name = "%guard-evt", lib = "(cml bridge)")]
pub async fn guard_evt_bridge(thunk: Procedure) -> Result<Vec<Value>, Exception> {
    let try_fn: TryFn = Arc::new(|| None);
    let thunk_clone = thunk.clone();
    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        let thunk = thunk_clone.clone();
        tokio::spawn(async move {
            let result = resolve_guard(&thunk).await;
            match result {
                Ok(evt_val) => {
                    if let Ok(inner) = evt_val.try_to_rust_type::<BaseEvent>() {
                        if let Some(value) = (inner.try_fn)() {
                            if cas(&flag, OpState::Waiting, OpState::Synched) {
                                let _ = tx.send(value);
                            }
                        } else {
                            (inner.block_fn)(flag, tx);
                        }
                    }
                }
                Err(_) => {}
            }
        });
    });
    let cancel_fn: CancelFn = Arc::new(|| {});

    let event = BaseEvent {
        try_fn,
        block_fn,
        cancel_fn,
        wrap_fn: None,
    };
    Ok(vec![Value::from_rust_type(event)])
}
```

**This is intentionally rough.** There are TODO markers for wrap composition over choices and a few edge cases. The structure is correct — the next tasks flesh out the details.

- [ ] **Step 3: Handle wrap composition**

The `wrap` over a `ChoiceEvent` needs to wrap each alternative. Since `wrap_bridge` is async but we need to process each alternative, handle it synchronously by constructing new BaseEvents:

In `wrap_bridge`, replace the ChoiceEvent branch:

```rust
    } else if let Ok(choice) = evt_val.try_to_rust_type::<ChoiceEvent>() {
        let mut wrapped_alts: Vec<Value> = Vec::new();
        for alt_val in &choice.alternatives {
            let event = alt_val.try_to_rust_type::<BaseEvent>()?;
            let new_wrap = match &event.wrap_fn {
                None => Some(transform.clone()),
                Some(inner) => {
                    // Compose: transform(inner(value))
                    let inner = inner.clone();
                    let outer = transform.clone();
                    // Create a wrapper procedure that chains them
                    // For now: just use the outermost. Full composition in task 2b.
                    Some(outer)
                }
            };
            let wrapped = BaseEvent {
                try_fn: event.try_fn.clone(),
                block_fn: event.block_fn.clone(),
                cancel_fn: event.cancel_fn.clone(),
                wrap_fn: new_wrap,
            };
            wrapped_alts.push(Value::from_rust_type(wrapped));
        }
        let choice = ChoiceEvent { alternatives: wrapped_alts };
        Ok(vec![Value::from_rust_type(choice)])
    }
```

For full wrap composition (chaining two Procedures), store a `Vec<Procedure>` instead of `Option<Procedure>` for `wrap_fn`, and apply them in sequence in `apply_wrap`. Replace:

```rust
pub struct BaseEvent {
    pub try_fn: TryFn,
    pub block_fn: BlockFn,
    pub cancel_fn: CancelFn,
    pub wrap_fns: Vec<Procedure>,
}
```

And update `apply_wrap`:

```rust
async fn apply_wraps(wrap_fns: &[Procedure], mut value: Value) -> Result<Value, Exception> {
    for proc in wrap_fns {
        let results = proc.call(&[value], &mut ContBarrier::new()).await?;
        value = results
            .into_iter()
            .next()
            .ok_or_else(|| Exception::error("wrap function returned no values"))?;
    }
    Ok(value)
}
```

And in `wrap_bridge`, composition is just appending:

```rust
let mut wraps = event.wrap_fns.clone();
wraps.push(transform);
let wrapped = BaseEvent {
    try_fn: event.try_fn.clone(),
    block_fn: event.block_fn.clone(),
    cancel_fn: event.cancel_fn.clone(),
    wrap_fns: wraps,
};
```

Update all references from `wrap_fn` to `wrap_fns` and from `apply_wrap` to `apply_wraps`.

- [ ] **Step 4: Verify compilation**

Run: `cargo check 2>&1 | tail -10`

Expected: compilation errors from other modules (`channels.rs`, `timers.rs`, etc.) that still reference the old `Event` enum. `event.rs` itself should compile.

- [ ] **Step 5: Commit**

```
git add src/event.rs
git commit -m "feat: BaseEvent + ChoiceEvent with PCML protocol, perform_operation"
```

---

### Task 3: Lock-Free Channels

**Files:**
- Rewrite: `src/channels.rs`

- [ ] **Step 1: Write the new channels.rs**

```rust
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use imbl::Vector;
use scheme_rs::exceptions::Exception;
use scheme_rs::gc::{OpaqueGcPtr, Trace};
use scheme_rs::records::{RecordTypeDescriptor, SchemeCompatible, rtd};
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;
use tokio::sync::oneshot;

use crate::event::{
    BaseEvent, BlockFn, CancelFn, Flag, OpState, ResumeTx, TryFn,
    cas, flag_state, new_flag,
};

struct SendWaiter {
    flag: Flag,
    tx: std::sync::Mutex<Option<ResumeTx>>,
    message: Value,
}

struct RecvWaiter {
    flag: Flag,
    tx: std::sync::Mutex<Option<ResumeTx>>,
}

pub struct CmlChannel {
    putq: ArcSwap<Vector<Arc<SendWaiter>>>,
    getq: ArcSwap<Vector<Arc<RecvWaiter>>>,
    putq_gc_counter: AtomicUsize,
    getq_gc_counter: AtomicUsize,
    capacity: Option<usize>,
    buffer: Option<ArcSwap<Vector<Value>>>,
}

const GC_INTERVAL: usize = 64;

impl CmlChannel {
    pub fn new_rendezvous() -> Self {
        Self {
            putq: ArcSwap::from_pointee(Vector::new()),
            getq: ArcSwap::from_pointee(Vector::new()),
            putq_gc_counter: AtomicUsize::new(GC_INTERVAL),
            getq_gc_counter: AtomicUsize::new(GC_INTERVAL),
            capacity: None,
            buffer: None,
        }
    }

    pub fn new_buffered(capacity: usize) -> Self {
        Self {
            putq: ArcSwap::from_pointee(Vector::new()),
            getq: ArcSwap::from_pointee(Vector::new()),
            putq_gc_counter: AtomicUsize::new(GC_INTERVAL),
            getq_gc_counter: AtomicUsize::new(GC_INTERVAL),
            capacity: Some(capacity),
            buffer: Some(ArcSwap::from_pointee(Vector::new())),
        }
    }

    fn gc_queue_senders(&self) {
        if self.putq_gc_counter.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.putq.rcu(|q| {
                let filtered: Vector<Arc<SendWaiter>> = q
                    .iter()
                    .filter(|w| flag_state(&w.flag) != OpState::Synched)
                    .cloned()
                    .collect();
                Arc::new(filtered)
            });
            self.putq_gc_counter.store(GC_INTERVAL, Ordering::Relaxed);
        }
    }

    fn gc_queue_receivers(&self) {
        if self.getq_gc_counter.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.getq.rcu(|q| {
                let filtered: Vector<Arc<RecvWaiter>> = q
                    .iter()
                    .filter(|w| flag_state(&w.flag) != OpState::Synched)
                    .cloned()
                    .collect();
                Arc::new(filtered)
            });
            self.getq_gc_counter.store(GC_INTERVAL, Ordering::Relaxed);
        }
    }
}

impl std::fmt::Debug for CmlChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CmlChannel")
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}

unsafe impl Trace for CmlChannel {
    unsafe fn visit_children(&self, _visitor: &mut dyn FnMut(OpaqueGcPtr)) {}

    unsafe fn finalize(&mut self) {
        unsafe { std::ptr::drop_in_place(self as *mut Self) }
    }
}

impl SchemeCompatible for CmlChannel {
    fn rtd() -> Arc<RecordTypeDescriptor> {
        rtd!(
            name: "cml-channel",
            opaque: true,
            sealed: true,
        )
    }
}

pub fn recv_event(channel: Arc<CmlChannel>) -> BaseEvent {
    let ch = channel.clone();
    let try_fn: TryFn = Arc::new(move || {
        // Buffered: check buffer first
        if let Some(ref buf) = ch.buffer {
            let guard = buf.load();
            if !guard.is_empty() {
                let mut new_buf = (**guard).clone();
                let value = new_buf.pop_front().unwrap();
                // CAS the buffer
                // Note: rcu would be cleaner but we need to return the value
                buf.rcu(|b| {
                    let mut b = (**b).clone();
                    b.pop_front();
                    Arc::new(b)
                });
                return Some(value);
            }
        }

        // Scan putq for a waiting sender
        let putq = ch.putq.load();
        for (idx, sender) in putq.iter().enumerate() {
            if cas(&sender.flag, OpState::Waiting, OpState::Synched) {
                let message = sender.message.clone();
                if let Some(tx) = sender.tx.lock().unwrap().take() {
                    let _ = tx.send(Value::from(false));
                }
                // Remove sender from queue
                ch.putq.rcu(|q| {
                    let mut q = (**q).clone();
                    q.retain(|w| !Arc::ptr_eq(w, sender));
                    Arc::new(q)
                });
                return Some(message);
            }
        }
        None
    });

    let ch = channel.clone();
    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        let waiter = Arc::new(RecvWaiter {
            flag: flag.clone(),
            tx: std::sync::Mutex::new(Some(tx)),
        });

        // Enqueue self
        let waiter_clone = waiter.clone();
        ch.getq.rcu(|q| {
            let mut q = (**q).clone();
            q.push_back(waiter_clone.clone());
            Arc::new(q)
        });

        ch.gc_queue_receivers();

        // Scan putq for a matching sender (double-claim protocol)
        let putq = ch.putq.load();
        for sender in putq.iter() {
            if flag_state(&sender.flag) == OpState::Synched {
                continue;
            }

            // Claim ourselves
            if !cas(&flag, OpState::Waiting, OpState::Claimed) {
                return; // another alternative in our choice won
            }

            // Try to synch the sender
            match sender.flag.compare_exchange(
                OpState::Waiting as u8,
                OpState::Synched as u8,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    // Won! Complete the rendezvous.
                    flag.store(OpState::Synched as u8, Ordering::SeqCst);
                    let message = sender.message.clone();
                    // Resume the sender
                    if let Some(stx) = sender.tx.lock().unwrap().take() {
                        let _ = stx.send(Value::from(false));
                    }
                    // Resume ourselves
                    if let Some(rtx) = waiter.tx.lock().unwrap().take() {
                        let _ = rtx.send(message);
                    }
                    return;
                }
                Err(v) if v == OpState::Claimed as u8 => {
                    // Sender is being claimed by someone else, release and retry
                    flag.store(OpState::Waiting as u8, Ordering::SeqCst);
                    continue;
                }
                Err(_) => {
                    // Sender already synched, release and skip
                    flag.store(OpState::Waiting as u8, Ordering::SeqCst);
                    continue;
                }
            }
        }

        // Also check buffer for buffered channels
        if let Some(ref buf) = ch.buffer {
            let guard = buf.load();
            if !guard.is_empty() {
                if cas(&flag, OpState::Waiting, OpState::Synched) {
                    let mut new_buf = (**guard).clone();
                    let value = new_buf.pop_front().unwrap();
                    buf.store(Arc::new(new_buf));
                    if let Some(rtx) = waiter.tx.lock().unwrap().take() {
                        let _ = rtx.send(value);
                    }
                }
            }
        }
    });

    let cancel_fn: CancelFn = Arc::new(|| {});

    BaseEvent {
        try_fn,
        block_fn,
        cancel_fn,
        wrap_fns: Vec::new(),
    }
}

pub fn send_event(channel: Arc<CmlChannel>, msg: Value) -> BaseEvent {
    let ch = channel.clone();
    let msg_clone = msg.clone();
    let try_fn: TryFn = Arc::new(move || {
        // Buffered: try to push to buffer
        if let (Some(ref buf), Some(cap)) = (&ch.buffer, ch.capacity) {
            let guard = buf.load();
            if guard.len() < cap {
                buf.rcu(|b| {
                    let mut b = (**b).clone();
                    b.push_back(msg_clone.clone());
                    Arc::new(b)
                });
                return Some(Value::from(false));
            }
        }

        // Scan getq for a waiting receiver
        let getq = ch.getq.load();
        for receiver in getq.iter() {
            if cas(&receiver.flag, OpState::Waiting, OpState::Synched) {
                if let Some(tx) = receiver.tx.lock().unwrap().take() {
                    let _ = tx.send(msg_clone.clone());
                }
                ch.getq.rcu(|q| {
                    let mut q = (**q).clone();
                    q.retain(|w| !Arc::ptr_eq(w, receiver));
                    Arc::new(q)
                });
                return Some(Value::from(false));
            }
        }
        None
    });

    let ch = channel.clone();
    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        let waiter = Arc::new(SendWaiter {
            flag: flag.clone(),
            tx: std::sync::Mutex::new(Some(tx)),
            message: msg.clone(),
        });

        let waiter_clone = waiter.clone();
        ch.putq.rcu(|q| {
            let mut q = (**q).clone();
            q.push_back(waiter_clone.clone());
            Arc::new(q)
        });

        ch.gc_queue_senders();

        // Scan getq for a matching receiver (double-claim protocol)
        let getq = ch.getq.load();
        for receiver in getq.iter() {
            if flag_state(&receiver.flag) == OpState::Synched {
                continue;
            }

            if !cas(&flag, OpState::Waiting, OpState::Claimed) {
                return;
            }

            match receiver.flag.compare_exchange(
                OpState::Waiting as u8,
                OpState::Synched as u8,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    flag.store(OpState::Synched as u8, Ordering::SeqCst);
                    if let Some(rtx) = receiver.tx.lock().unwrap().take() {
                        let _ = rtx.send(msg.clone());
                    }
                    if let Some(stx) = waiter.tx.lock().unwrap().take() {
                        let _ = stx.send(Value::from(false));
                    }
                    return;
                }
                Err(v) if v == OpState::Claimed as u8 => {
                    flag.store(OpState::Waiting as u8, Ordering::SeqCst);
                    continue;
                }
                Err(_) => {
                    flag.store(OpState::Waiting as u8, Ordering::SeqCst);
                    continue;
                }
            }
        }
    });

    let cancel_fn: CancelFn = Arc::new(|| {});

    BaseEvent {
        try_fn,
        block_fn,
        cancel_fn,
        wrap_fns: Vec::new(),
    }
}

// --- Bridges ---

#[bridge(name = "%make-rendezvous-channel", lib = "(cml channels bridge)")]
pub async fn make_rendezvous_channel() -> Result<Vec<Value>, Exception> {
    Ok(vec![Value::from_rust_type(CmlChannel::new_rendezvous())])
}

#[bridge(name = "%make-buffered-channel", lib = "(cml channels bridge)")]
pub async fn make_buffered_channel(capacity: usize) -> Result<Vec<Value>, Exception> {
    if capacity == 0 {
        return Err(Exception::error("buffered channel capacity must be > 0"));
    }
    Ok(vec![Value::from_rust_type(CmlChannel::new_buffered(capacity))])
}

#[bridge(name = "%send-evt", lib = "(cml channels bridge)")]
pub async fn send_evt_bridge(ch_val: &Value, msg: &Value) -> Result<Vec<Value>, Exception> {
    let channel = ch_val.try_to_rust_type::<CmlChannel>()?;
    // We need an Arc<CmlChannel> — extract from Gc
    // Gc<CmlChannel> deref gives &CmlChannel. We need shared ownership.
    // Wrap in Arc at channel creation time, or use Gc as the shared handle.
    // For now: clone the inner channel data into an Arc.
    // TODO: resolve Gc vs Arc ownership — see task notes.
    todo!("need Arc<CmlChannel> for closures")
}

#[bridge(name = "%recv-evt", lib = "(cml channels bridge)")]
pub async fn recv_evt_bridge(ch_val: &Value) -> Result<Vec<Value>, Exception> {
    let channel = ch_val.try_to_rust_type::<CmlChannel>()?;
    todo!("need Arc<CmlChannel> for closures")
}
```

**Design issue: Gc vs Arc for channel sharing.** The `recv_event` and `send_event` functions need `Arc<CmlChannel>` because the closures (try_fn, block_fn) need `'static` references to the channel. But scheme-rs stores channels as `Gc<CmlChannel>`. We need to resolve this.

**Resolution:** Store the channel internals (queues, buffer) in an `Arc<ChannelInner>` which both the `Gc`-managed `CmlChannel` (for Scheme) and the closures share. The `CmlChannel` is a thin wrapper:

```rust
pub struct ChannelInner {
    putq: ArcSwap<Vector<Arc<SendWaiter>>>,
    getq: ArcSwap<Vector<Arc<RecvWaiter>>>,
    putq_gc_counter: AtomicUsize,
    getq_gc_counter: AtomicUsize,
    capacity: Option<usize>,
    buffer: Option<ArcSwap<Vector<Value>>>,
}

#[derive(Clone)]
pub struct CmlChannel {
    inner: Arc<ChannelInner>,
}
```

Now `CmlChannel` is cheaply cloneable, and closures capture `CmlChannel` (which is just an `Arc` bump). The `SchemeCompatible` impl wraps it in a Gc record, but the closures hold Arc clones.

Update `recv_event` and `send_event` to take `CmlChannel` (which contains `Arc<ChannelInner>`) instead of `Arc<CmlChannel>`. Update bridges:

```rust
#[bridge(name = "%send-evt", lib = "(cml channels bridge)")]
pub async fn send_evt_bridge(ch_val: &Value, msg: &Value) -> Result<Vec<Value>, Exception> {
    let channel = ch_val.try_to_rust_type::<CmlChannel>()?;
    let event = send_event((*channel).clone(), msg.clone());
    Ok(vec![Value::from_rust_type(event)])
}

#[bridge(name = "%recv-evt", lib = "(cml channels bridge)")]
pub async fn recv_evt_bridge(ch_val: &Value) -> Result<Vec<Value>, Exception> {
    let channel = ch_val.try_to_rust_type::<CmlChannel>()?;
    let event = recv_event((*channel).clone());
    Ok(vec![Value::from_rust_type(event)])
}
```

- [ ] **Step 2: Verify channels.rs compiles**

Run: `cargo check 2>&1 | tail -10`

- [ ] **Step 3: Commit**

```
git add src/channels.rs
git commit -m "feat: lock-free channels with PCML double-claim protocol"
```

---

### Task 4: Timer, Condition, Notifier, Custom Events

**Files:**
- Rewrite: `src/timers.rs`
- Rewrite: `src/conditions.rs`
- Rewrite: `src/custom.rs`

- [ ] **Step 1: Write src/timers.rs**

```rust
use std::sync::Arc;

use scheme_rs::exceptions::Exception;
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;

use crate::event::{BaseEvent, BlockFn, CancelFn, Flag, OpState, ResumeTx, TryFn, cas};

#[bridge(name = "%sleep-evt", lib = "(cml timers bridge)")]
pub async fn sleep_evt(seconds: f64) -> Result<Vec<Value>, Exception> {
    let nanos = (seconds * 1_000_000_000.0) as u64;
    let duration = std::time::Duration::from_nanos(nanos);

    let try_fn: TryFn = if duration.is_zero() {
        Arc::new(|| Some(Value::from(false)))
    } else {
        Arc::new(|| None)
    };

    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            if cas(&flag, OpState::Waiting, OpState::Synched) {
                let _ = tx.send(Value::from(false));
            }
        });
    });

    let cancel_fn: CancelFn = Arc::new(|| {});

    let event = BaseEvent {
        try_fn,
        block_fn,
        cancel_fn,
        wrap_fns: Vec::new(),
    };
    Ok(vec![Value::from_rust_type(event)])
}
```

- [ ] **Step 2: Write src/conditions.rs**

```rust
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use scheme_rs::exceptions::Exception;
use scheme_rs::gc::{OpaqueGcPtr, Trace};
use scheme_rs::records::{rtd, RecordTypeDescriptor, SchemeCompatible};
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;
use tokio::sync::{Notify, Semaphore};

use crate::event::{BaseEvent, BlockFn, CancelFn, Flag, OpState, ResumeTx, TryFn, cas};

#[derive(Debug, Clone)]
pub struct Condition {
    pub notify: Arc<Notify>,
    pub signalled: Arc<AtomicBool>,
}

unsafe impl Trace for Condition {
    unsafe fn visit_children(&self, _visitor: &mut dyn FnMut(OpaqueGcPtr)) {}
    unsafe fn finalize(&mut self) {
        unsafe { std::ptr::drop_in_place(self as *mut Self) }
    }
}

impl SchemeCompatible for Condition {
    fn rtd() -> Arc<RecordTypeDescriptor> {
        rtd!(name: "cml-condition", opaque: true, sealed: true)
    }
}

#[derive(Debug, Clone)]
pub struct CmlNotifier {
    pub sem: Arc<Semaphore>,
}

unsafe impl Trace for CmlNotifier {
    unsafe fn visit_children(&self, _visitor: &mut dyn FnMut(OpaqueGcPtr)) {}
    unsafe fn finalize(&mut self) {
        unsafe { std::ptr::drop_in_place(self as *mut Self) }
    }
}

impl SchemeCompatible for CmlNotifier {
    fn rtd() -> Arc<RecordTypeDescriptor> {
        rtd!(name: "cml-notifier", opaque: true, sealed: true)
    }
}

#[bridge(name = "%make-condition", lib = "(cml conditions bridge)")]
pub async fn make_condition() -> Result<Vec<Value>, Exception> {
    let cond = Condition {
        notify: Arc::new(Notify::new()),
        signalled: Arc::new(AtomicBool::new(false)),
    };
    Ok(vec![Value::from_rust_type(cond)])
}

#[bridge(name = "%signal!", lib = "(cml conditions bridge)")]
pub async fn signal(cv_val: &Value) -> Result<Vec<Value>, Exception> {
    let cv = cv_val.try_to_rust_type::<Condition>()?;
    let was_first = !cv.signalled.swap(true, Ordering::SeqCst);
    if was_first {
        cv.notify.notify_waiters();
    }
    Ok(vec![Value::from(was_first)])
}

#[bridge(name = "%wait-evt", lib = "(cml conditions bridge)")]
pub async fn wait_evt(cv_val: &Value) -> Result<Vec<Value>, Exception> {
    let cv = cv_val.try_to_rust_type::<Condition>()?;
    let signalled = cv.signalled.clone();
    let notify = cv.notify.clone();

    let try_fn: TryFn = Arc::new(move || {
        if signalled.load(Ordering::SeqCst) {
            Some(Value::from(true))
        } else {
            None
        }
    });

    let signalled = cv.signalled.clone();
    let notify = cv.notify.clone();
    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        let signalled = signalled.clone();
        let notify = notify.clone();
        tokio::spawn(async move {
            if signalled.load(Ordering::SeqCst) {
                if cas(&flag, OpState::Waiting, OpState::Synched) {
                    let _ = tx.send(Value::from(true));
                }
                return;
            }
            notify.notified().await;
            if cas(&flag, OpState::Waiting, OpState::Synched) {
                let _ = tx.send(Value::from(true));
            }
        });
    });

    let cancel_fn: CancelFn = Arc::new(|| {});

    let event = BaseEvent {
        try_fn,
        block_fn,
        cancel_fn,
        wrap_fns: Vec::new(),
    };
    Ok(vec![Value::from_rust_type(event)])
}

#[bridge(name = "%make-notifier", lib = "(cml conditions bridge)")]
pub async fn make_notifier() -> Result<Vec<Value>, Exception> {
    let n = CmlNotifier {
        sem: Arc::new(Semaphore::new(0)),
    };
    Ok(vec![Value::from_rust_type(n)])
}

#[bridge(name = "%notify!", lib = "(cml conditions bridge)")]
pub async fn notify(n_val: &Value) -> Result<Vec<Value>, Exception> {
    let n = n_val.try_to_rust_type::<CmlNotifier>()?;
    n.sem.add_permits(1);
    Ok(vec![])
}

#[bridge(name = "%notify-evt", lib = "(cml conditions bridge)")]
pub async fn notify_evt(n_val: &Value) -> Result<Vec<Value>, Exception> {
    let n = n_val.try_to_rust_type::<CmlNotifier>()?;
    let sem = n.sem.clone();

    let sem_try = sem.clone();
    let try_fn: TryFn = Arc::new(move || {
        match sem_try.try_acquire() {
            Ok(permit) => {
                permit.forget();
                Some(Value::from(true))
            }
            Err(_) => None,
        }
    });

    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        let sem = sem.clone();
        tokio::spawn(async move {
            match sem.acquire().await {
                Ok(permit) => {
                    if cas(&flag, OpState::Waiting, OpState::Synched) {
                        permit.forget();
                        let _ = tx.send(Value::from(true));
                    } else {
                        // Lost the choice — put the permit back
                        drop(permit);
                        sem.add_permits(1);
                    }
                }
                Err(_) => {}
            }
        });
    });

    let cancel_fn: CancelFn = Arc::new(|| {});

    let event = BaseEvent {
        try_fn,
        block_fn,
        cancel_fn,
        wrap_fns: Vec::new(),
    };
    Ok(vec![Value::from_rust_type(event)])
}
```

- [ ] **Step 3: Write src/custom.rs**

```rust
use std::sync::Arc;

use scheme_rs::exceptions::Exception;
use scheme_rs::proc::{ContBarrier, Procedure};
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;

use crate::event::{BaseEvent, BlockFn, CancelFn, Flag, OpState, ResumeTx, TryFn, cas};

#[bridge(name = "%make-custom-event", lib = "(cml bridge)")]
pub async fn make_custom_event(thunk: Procedure) -> Result<Vec<Value>, Exception> {
    let thunk_try = thunk.clone();
    let try_fn: TryFn = Arc::new(move || {
        // Custom events are always ready — but try_fn is sync and thunk.call is async.
        // We can't call async from try_fn. So custom events always go to block path.
        None
    });

    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        let thunk = thunk.clone();
        tokio::spawn(async move {
            match thunk.call(&[], &mut ContBarrier::new()).await {
                Ok(results) => {
                    if cas(&flag, OpState::Waiting, OpState::Synched) {
                        let value = results.into_iter().next().unwrap_or(Value::from(false));
                        let _ = tx.send(value);
                    }
                }
                Err(_) => {}
            }
        });
    });

    let cancel_fn: CancelFn = Arc::new(|| {});

    let event = BaseEvent {
        try_fn,
        block_fn,
        cancel_fn,
        wrap_fns: Vec::new(),
    };
    Ok(vec![Value::from_rust_type(event)])
}
```

- [ ] **Step 4: Verify all three compile**

Run: `cargo check 2>&1 | tail -10`

- [ ] **Step 5: Commit**

```
git add src/timers.rs src/conditions.rs src/custom.rs
git commit -m "feat: timer, condition, notifier, custom events as BaseEvents"
```

---

### Task 5: Remove Tasks, Update lib.rs and Producer

**Files:**
- Delete: `src/tasks.rs`
- Modify: `src/lib.rs`
- Modify: `src/producer.rs`

- [ ] **Step 1: Update src/lib.rs**

```rust
pub mod channels;
pub mod conditions;
pub mod custom;
pub mod event;
pub mod producer;
pub mod timers;
```

- [ ] **Step 2: Delete src/tasks.rs**

```bash
rm src/tasks.rs
```

- [ ] **Step 3: Update src/producer.rs**

Adapt to use the new `CmlChannel` with `Arc<ChannelInner>`:

```rust
use scheme_rs::exceptions::Exception;
use scheme_rs::value::Value;

use crate::channels::CmlChannel;

pub struct CmlProducer {
    channel: CmlChannel,
}

impl CmlProducer {
    pub fn from_channel_value(val: &Value) -> Result<Self, Exception> {
        let ch = val.try_to_rust_type::<CmlChannel>()?;
        Ok(Self {
            channel: (*ch).clone(),
        })
    }

    pub fn try_send(&self, val: Value) -> Result<(), Exception> {
        // Use the channel's buffer if available, otherwise fail
        if let Some(ref buf) = self.channel.inner.buffer {
            if let Some(cap) = self.channel.inner.capacity {
                let guard = buf.load();
                if guard.len() < cap {
                    buf.rcu(|b| {
                        let mut b = (**b).clone();
                        b.push_back(val.clone());
                        std::sync::Arc::new(b)
                    });
                    return Ok(());
                }
            }
        }
        Err(Exception::error("channel full or closed"))
    }

    pub async fn send(&self, val: Value) -> Result<(), Exception> {
        // Create a send event and perform it
        let event = crate::channels::send_event(self.channel.clone(), val);
        crate::event::perform_base(&event).await?;
        Ok(())
    }
}

pub struct CmlConsumer {
    channel: CmlChannel,
}

impl CmlConsumer {
    pub fn from_channel_value(val: &Value) -> Result<Self, Exception> {
        let ch = val.try_to_rust_type::<CmlChannel>()?;
        Ok(Self {
            channel: (*ch).clone(),
        })
    }

    pub async fn recv(&self) -> Result<Value, Exception> {
        let event = crate::channels::recv_event(self.channel.clone());
        crate::event::perform_base(&event).await
    }
}
```

- [ ] **Step 4: Verify compilation**

Run: `cargo check 2>&1 | tail -10`

- [ ] **Step 5: Commit**

```
git add src/lib.rs src/producer.rs
git rm src/tasks.rs
git commit -m "refactor: remove tasks module, adapt producer to new channels"
```

---

### Task 6: Update Scheme Libraries

**Files:**
- Modify: `scheme/cml.sls`

- [ ] **Step 1: Update scheme/cml.sls**

```scheme
(library (cml)
  (export sync choose wrap guard-evt
          make-custom-event)
  (import (rnrs) (cml bridge))

  (define (sync evt) (%sync evt))
  (define (choose . evts) (apply %choose evts))
  (define (wrap evt f) (%wrap evt f))
  (define (guard-evt thunk) (%guard-evt thunk))
  (define (make-custom-event thunk) (%make-custom-event thunk)))
```

- [ ] **Step 2: Verify the scheme files are consistent**

`scheme/cml/channels.sls`, `scheme/cml/conditions.sls`, and `scheme/cml/timers.sls` should be unchanged — they reference the same bridge names. Verify:

Run: `grep '%' scheme/cml/channels.sls scheme/cml/conditions.sls scheme/cml/timers.sls`

Expected: bridge names match what's defined in the Rust code.

- [ ] **Step 3: Commit**

```
git add scheme/cml.sls
git commit -m "refactor: remove task exports from (cml) library"
```

---

### Task 7: Update Tests

**Files:**
- Modify: `tests/integration.rs`
- Modify: `tests/cml_tasks.scm`
- Modify: `tests/cml_stress_tasks.scm`
- Modify: `tests/cml_stress_channels.scm`
- Modify: `tests/cml_integration.scm`
- Modify: `tests/cml_api_coverage.scm`
- Delete: `tests/gc_crash_choose.scm` (becomes a regular stress test)
- Delete: `tests/gc_crash_async.scm` (no longer needed)

- [ ] **Step 1: Update tests/integration.rs**

Remove task-related tests, remove gc_crash investigation tests, keep all other test entries:

```rust
use scheme_rs::runtime::Runtime;
use scheme_rs::value::Value;
use scheme_rs_cml::channels::CmlChannel;
use scheme_rs_cml::producer::{CmlConsumer, CmlProducer};
use scheme_rs_cml as _;
use std::path::PathBuf;

fn run_scheme_test(filename: &str) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let scheme_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scheme");
        unsafe { std::env::set_var("SCHEME_RS_LOAD_PATH", &scheme_dir) };

        let runtime = Runtime::new();
        let test_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join(filename);
        runtime
            .run_program(&test_path)
            .await
            .expect(&format!("scheme test {filename} failed"));
    });
}

#[test]
fn test_cml_basic() {
    run_scheme_test("cml_basic.scm");
}

#[test]
fn test_cml_channels() {
    run_scheme_test("cml_channels.scm");
}

#[test]
fn test_cml_conditions() {
    run_scheme_test("cml_conditions.scm");
}

#[test]
fn test_cml_compose() {
    run_scheme_test("cml_compose.scm");
}

#[test]
fn test_cml_tasks() {
    run_scheme_test("cml_tasks.scm");
}

#[test]
fn test_cml_integration() {
    run_scheme_test("cml_integration.scm");
}

#[test]
fn test_cml_stress_channels() {
    run_scheme_test("cml_stress_channels.scm");
}

#[test]
fn test_cml_stress_choose() {
    run_scheme_test("cml_stress_choose.scm");
}

#[test]
fn test_cml_stress_tasks() {
    run_scheme_test("cml_stress_tasks.scm");
}

#[test]
fn test_cml_stress_conditions() {
    run_scheme_test("cml_stress_conditions.scm");
}

#[test]
fn test_cml_stress_mixed() {
    run_scheme_test("cml_stress_mixed.scm");
}

#[test]
fn test_cml_api_coverage() {
    run_scheme_test("cml_api_coverage.scm");
}

#[tokio::test]
async fn test_producer_consumer_roundtrip() {
    let ch = CmlChannel::new_buffered(10);
    let val = Value::from_rust_type(ch);
    let producer = CmlProducer::from_channel_value(&val).unwrap();
    let consumer = CmlConsumer::from_channel_value(&val).unwrap();

    producer.send(Value::from(42i64)).await.unwrap();
    let result = consumer.recv().await.unwrap();
    assert_eq!(result, Value::from(42i64));
}

#[tokio::test]
async fn test_producer_try_send_full() {
    let ch = CmlChannel::new_buffered(1);
    let val = Value::from_rust_type(ch);
    let producer = CmlProducer::from_channel_value(&val).unwrap();

    producer.try_send(Value::from(1i64)).unwrap();
    let err = producer.try_send(Value::from(2i64));
    assert!(err.is_err());
}

#[tokio::test]
async fn test_producer_consumer_multiple() {
    let ch = CmlChannel::new_buffered(10);
    let val = Value::from_rust_type(ch);
    let producer = CmlProducer::from_channel_value(&val).unwrap();
    let consumer = CmlConsumer::from_channel_value(&val).unwrap();

    for i in 0..10i64 {
        producer.send(Value::from(i)).await.unwrap();
    }
    for i in 0..10i64 {
        let result = consumer.recv().await.unwrap();
        assert_eq!(result, Value::from(i));
    }
}
```

Note: `CmlChannel::new_buffered` returns a `CmlChannel` directly (not wrapped in Value). The `Value::from_rust_type(ch)` call wraps it for the producer/consumer API.

- [ ] **Step 2: Rewrite task-dependent test files to use (async)**

All tests that used `run-tasks`/`spawn-task`/`yield` need rewriting to use `(import (async))` with `spawn`/`await`/`sleep`.

**tests/cml_tasks.scm** — rewrite to use `(async)`:

```scheme
(import (rnrs) (prefix (async) tokio/) (cml channels))

(let ((ch (make-channel 1)))
  (let ((f (tokio/spawn (lambda () (send ch 42)))))
    (assert (= (recv ch) 42)))
  (display "spawn-send-recv passed\n"))

(let ((ch (make-channel)))
  (let ((f (tokio/spawn (lambda () (send ch 'hello)))))
    (assert (eq? (recv ch) 'hello)))
  (display "rendezvous-channel passed\n"))

(let ((ch (make-channel 10))
      (done (make-channel 1)))
  (do ((i 0 (+ i 1)))
      ((= i 5))
    (tokio/spawn (lambda () (send ch i))))
  (tokio/sleep 100)
  (let loop ((count 0) (sum 0))
    (if (= count 5)
        (begin
          (assert (= sum 10))
          (display "multi-spawn-drain passed\n"))
        (loop (+ count 1) (+ sum (recv ch))))))

(display "all task tests passed\n")
```

**tests/cml_stress_tasks.scm** — rewrite:

```scheme
(import (rnrs) (prefix (async) tokio/) (cml) (cml channels))

(let ((ch (make-channel 200))
      (result-ch (make-channel 1)))
  (do ((i 0 (+ i 1)))
      ((= i 200))
    (let ((idx i))
      (tokio/spawn (lambda () (send ch idx)))))

  (tokio/spawn
    (lambda ()
      (let loop ((count 0) (sum 0))
        (if (= count 200)
            (send result-ch (cons count sum))
            (loop (+ count 1) (+ sum (recv ch)))))))

  (let ((result (recv result-ch)))
    (assert (= (car result) 200))
    (assert (= (cdr result) 19900)))
  (display "spawn-200 passed\n"))

(let ((result-ch (make-channel 1)))
  (let ((chs (let loop ((i 0) (acc '()))
               (if (= i 11)
                   (reverse acc)
                   (loop (+ i 1) (cons (make-channel 1) acc))))))
    (let ((input-ch (car chs))
          (output-ch (list-ref chs 10)))
      (do ((stage 0 (+ stage 1)))
          ((= stage 10))
        (let ((in-ch (list-ref chs stage))
              (out-ch (list-ref chs (+ stage 1))))
          (tokio/spawn
            (lambda ()
              (send out-ch (+ (recv in-ch) 1))))))
      (send input-ch 0)
      (let ((result (recv output-ch)))
        (send result-ch result))))
  (let ((result (recv result-ch)))
    (assert (= result 10)))
  (display "pipeline passed\n"))

(display "all stress-tasks tests passed\n")
```

Note: the yield-fairness test is removed — it depended on `yield` and cooperative scheduling within `run-tasks`. With tokio tasks, fairness is handled by the tokio scheduler.

**tests/cml_stress_channels.scm** — update rpc-fib and pingpong to use `(async)` spawn instead of `run-tasks`/`spawn-task`:

```scheme
(import (rnrs) (prefix (async) tokio/) (cml) (cml channels))

(define (rpc-fib n)
  (if (< n 2)
      n
      (let ((ch1 (make-channel 1))
            (ch2 (make-channel 1)))
        (tokio/spawn (lambda () (send ch1 (rpc-fib (- n 1)))))
        (tokio/spawn (lambda () (send ch2 (rpc-fib (- n 2)))))
        (+ (recv ch1) (recv ch2)))))

(let ((result (rpc-fib 15)))
  (assert (= result 610))
  (display "rpc-fib passed\n"))

(let ((request-ch (make-channel))
      (done-ch (make-channel 5)))
  (tokio/spawn
    (lambda ()
      (let loop ((served 0))
        (when (< served 500)
          (let ((reply-ch (recv request-ch)))
            (send reply-ch 'pong)
            (loop (+ served 1)))))))

  (do ((client 0 (+ client 1)))
      ((= client 5))
    (tokio/spawn
      (lambda ()
        (do ((round 0 (+ round 1)))
            ((= round 100))
          (let ((reply-ch (make-channel 1)))
            (send request-ch reply-ch)
            (let ((response (recv reply-ch)))
              (assert (eq? response 'pong)))))
        (send done-ch 'ok))))

  (do ((i 0 (+ i 1)))
      ((= i 5))
    (recv done-ch))
  (display "pingpong passed\n"))

(let ((ch (make-channel 10))
      (result-ch (make-channel 1)))
  (do ((p 0 (+ p 1)))
      ((= p 20))
    (tokio/spawn
      (lambda ()
        (do ((v 0 (+ v 1)))
            ((= v 50))
          (send ch v)))))

  (tokio/spawn
    (lambda ()
      (let loop ((count 0) (sum 0))
        (if (= count 1000)
            (send result-ch (cons count sum))
            (let ((v (recv ch)))
              (loop (+ count 1) (+ sum v)))))))

  (let ((result (recv result-ch)))
    (assert (= (car result) 1000))
    (assert (= (cdr result) 24500)))
  (display "fan-in passed\n"))

(display "all stress-channels tests passed\n")
```

**tests/cml_integration.scm** — update the paint-loop test to use `(async)`:

Read the current file to see what needs changing, then rewrite parts that use `run-tasks`/`spawn-task`.

**tests/cml_api_coverage.scm** — remove `run-tasks`, `spawn-task`, `yield` tests. Replace with `(async)` equivalents where applicable. Remove the tests for removed exports entirely.

**tests/cml_stress_mixed.scm** — update the "wrap-choose-guard-load" test that uses `run-tasks`/`spawn-task`.

- [ ] **Step 3: Delete investigation test files**

```bash
rm tests/gc_crash_choose.scm tests/gc_crash_async.scm
```

- [ ] **Step 4: Run the full test suite**

Run: `cargo test 2>&1`

Expected: all tests pass with no segfaults. This is the primary success criterion.

- [ ] **Step 5: Run stress_choose 30 times to verify no GC crash**

Run: `for i in $(seq 1 30); do cargo test --test integration test_cml_stress_choose -- --test-threads=1 2>&1 | tail -1; done`

Expected: 0 crashes out of 30.

- [ ] **Step 6: Commit**

```
git add tests/
git commit -m "test: rewrite all tests for PCML protocol, remove task tests"
```

---

### Task 8: Final Verification and Cleanup

- [ ] **Step 1: Run full test suite with parallel threads**

Run: `cargo test 2>&1`

Expected: all tests pass, no segfaults.

- [ ] **Step 2: Run cargo clippy**

Run: `cargo clippy 2>&1 | tail -20`

Fix any warnings.

- [ ] **Step 3: Verify no old code remains**

Run: `rg 'select_all\|perform_owned\|BoxFuture\|futures::future' src/`

Expected: no matches.

Run: `rg 'run.tasks\|spawn.task\|%yield\|%spawn\|%run' src/ scheme/`

Expected: no matches.

- [ ] **Step 4: Run stress tests in a loop**

Run: `for i in $(seq 1 50); do result=$(cargo test 2>&1 | tail -3); if echo "$result" | grep -q 'signal:'; then echo "CRASH $i"; fi; done; echo "done"`

Expected: 0 crashes out of 50.

- [ ] **Step 5: Commit**

```
git add -A
git commit -m "chore: cleanup after PCML redesign"
```
