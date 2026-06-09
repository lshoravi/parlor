use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use rand::seq::SliceRandom;
use scheme_rs::exceptions::Exception;
use scheme_rs::gc::{Gc, OpaqueGcPtr, Trace};
use scheme_rs::proc::{ContBarrier, Procedure};
use scheme_rs::records::{RecordTypeDescriptor, SchemeCompatible, rtd};
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;
use tokio::sync::{Mutex, Notify, oneshot};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpState {
    Waiting = 0,
    Claimed = 1,
    Synched = 2,
}

pub type Flag = Arc<AtomicU8>;
pub type ResumeTx = oneshot::Sender<Value>;

pub fn new_flag() -> Flag {
    Arc::new(AtomicU8::new(OpState::Waiting as u8))
}

pub fn cas(flag: &Flag, expected: OpState, desired: OpState) -> bool {
    flag.compare_exchange(
        expected as u8,
        desired as u8,
        Ordering::AcqRel,
        Ordering::Acquire,
    )
    .is_ok()
}

pub fn flag_state(flag: &Flag) -> OpState {
    match flag.load(Ordering::Acquire) {
        0 => OpState::Waiting,
        1 => OpState::Claimed,
        2 => OpState::Synched,
        _ => unreachable!(),
    }
}

pub type PollFn = Arc<dyn Fn() -> bool + Send + Sync>;
pub type DoFn = Arc<dyn Fn() -> Option<Value> + Send + Sync>;
pub type BlockFn = Arc<dyn Fn(Flag, ResumeTx) + Send + Sync>;
pub type CancelFn = Arc<dyn Fn() + Send + Sync>;

pub fn make_abort_cancel() -> (Arc<std::sync::Mutex<Option<tokio::task::AbortHandle>>>, CancelFn) {
    let slot: Arc<std::sync::Mutex<Option<tokio::task::AbortHandle>>> =
        Arc::new(std::sync::Mutex::new(None));
    let slot_for_cancel = slot.clone();
    let cancel_fn: CancelFn = Arc::new(move || {
        if let Some(h) = slot_for_cancel.lock().unwrap().take() {
            h.abort();
        }
    });
    (slot, cancel_fn)
}

pub fn make_flag_cancel() -> (Arc<std::sync::Mutex<Option<Flag>>>, CancelFn) {
    let slot: Arc<std::sync::Mutex<Option<Flag>>> = Arc::new(std::sync::Mutex::new(None));
    let slot_for_cancel = slot.clone();
    let cancel_fn: CancelFn = Arc::new(move || {
        if let Some(ref flag) = *slot_for_cancel.lock().unwrap() {
            let _ = flag.compare_exchange(
                OpState::Waiting as u8,
                OpState::Synched as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    });
    (slot, cancel_fn)
}

pub struct BaseEvent {
    pub poll_fn: PollFn,
    pub do_fn: DoFn,
    pub block_fn: BlockFn,
    pub cancel_fn: CancelFn,
    pub wrap_fns: Vec<Procedure>,
}

impl std::fmt::Debug for BaseEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BaseEvent")
            .field("wrap_fns", &self.wrap_fns.len())
            .finish_non_exhaustive()
    }
}

unsafe impl Trace for BaseEvent {
    unsafe fn visit_children(&self, visitor: &mut dyn FnMut(OpaqueGcPtr)) {
        for wrap in &self.wrap_fns {
            unsafe { wrap.visit_children(visitor) };
        }
    }

    unsafe fn finalize(&mut self) {
        unsafe {
            std::ptr::drop_in_place(&mut self.poll_fn);
            std::ptr::drop_in_place(&mut self.do_fn);
            std::ptr::drop_in_place(&mut self.block_fn);
            std::ptr::drop_in_place(&mut self.cancel_fn);
            // SAFETY: GC calls visit_children (decrementing Gc refcounts) before
            // finalize — see collection.rs release() and free_cycle(). Clear length
            // to prevent Drop from double-decrementing, then drop the buffer.
            self.wrap_fns.set_len(0);
            std::ptr::drop_in_place(&mut self.wrap_fns);
        }
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

#[derive(Debug)]
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
        unsafe {
            // SAFETY: GC calls visit_children (decrementing Gc refcounts) before
            // finalize — see collection.rs release() and free_cycle(). Clear length
            // to prevent Drop from double-decrementing, then drop the buffer.
            self.alternatives.set_len(0);
            std::ptr::drop_in_place(&mut self.alternatives);
        }
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

struct FlagGuard(Flag);

impl Drop for FlagGuard {
    fn drop(&mut self) {
        let _ = self.0.compare_exchange(
            OpState::Waiting as u8,
            OpState::Synched as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}

pub async fn perform_base(event: &BaseEvent) -> Result<Value, Exception> {
    if (event.poll_fn)() {
        if let Some(value) = (event.do_fn)() {
            return apply_wraps(&event.wrap_fns, value).await;
        }
    }

    let flag = new_flag();
    let _guard = FlagGuard(flag.clone());
    let (tx, rx) = oneshot::channel();
    (event.block_fn)(flag, tx);
    let value = rx
        .await
        .map_err(|_| Exception::error("operation cancelled"))?;
    apply_wraps(&event.wrap_fns, value).await
}

pub async fn perform_choice(choice: &ChoiceEvent) -> Result<Value, Exception> {
    let alts: Vec<Gc<BaseEvent>> = choice
        .alternatives
        .iter()
        .map(|v| v.try_to_rust_type::<BaseEvent>())
        .collect::<Result<Vec<_>, _>>()?;

    if alts.is_empty() {
        return Err(Exception::error("choose: no alternatives"));
    }
    if alts.len() == 1 {
        return perform_base(&alts[0]).await;
    }

    let mut enabled: Vec<usize> = alts
        .iter()
        .enumerate()
        .filter(|(_, alt)| (alt.poll_fn)())
        .map(|(i, _)| i)
        .collect();
    enabled.shuffle(&mut rand::rng());
    for &i in &enabled {
        if let Some(value) = (alts[i].do_fn)() {
            return apply_wraps(&alts[i].wrap_fns, value).await;
        }
    }

    let flag = new_flag();
    let _guard = FlagGuard(flag.clone());
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

    apply_wraps(&alts[winner_index].wrap_fns, value).await
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
        let mut wraps = event.wrap_fns.clone();
        wraps.push(transform);
        let wrapped = BaseEvent {
            poll_fn: event.poll_fn.clone(),
            do_fn: event.do_fn.clone(),
            block_fn: event.block_fn.clone(),
            cancel_fn: event.cancel_fn.clone(),
            wrap_fns: wraps,
        };
        return Ok(vec![Value::from_rust_type(wrapped)]);
    }
    if let Ok(choice) = evt_val.try_to_rust_type::<ChoiceEvent>() {
        let mut wrapped_alts: Vec<Value> = Vec::new();
        for alt_val in &choice.alternatives {
            let base = alt_val.try_to_rust_type::<BaseEvent>()?;
            let mut wraps = base.wrap_fns.clone();
            wraps.push(transform.clone());
            let wrapped = BaseEvent {
                poll_fn: base.poll_fn.clone(),
                do_fn: base.do_fn.clone(),
                block_fn: base.block_fn.clone(),
                cancel_fn: base.cancel_fn.clone(),
                wrap_fns: wraps,
            };
            wrapped_alts.push(Value::from_rust_type(wrapped));
        }
        let choice = ChoiceEvent {
            alternatives: wrapped_alts,
        };
        return Ok(vec![Value::from_rust_type(choice)]);
    }
    Err(Exception::error("wrap: expected an event"))
}

#[bridge(name = "%choose", lib = "(cml bridge)")]
pub async fn choose_bridge(evts: &[Value]) -> Result<Vec<Value>, Exception> {
    let mut alternatives: Vec<Value> = Vec::new();
    for v in evts {
        if v.try_to_rust_type::<BaseEvent>().is_ok() {
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

fn guard_sync_choice(choice: &ChoiceEvent, flag: Flag, tx: ResumeTx) {
    let alts: Vec<Gc<BaseEvent>> = match choice
        .alternatives
        .iter()
        .map(|v| v.try_to_rust_type::<BaseEvent>())
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(a) => a,
        Err(_) => return,
    };

    let mut indices: Vec<usize> = (0..alts.len()).collect();
    indices.shuffle(&mut rand::rng());
    for &i in &indices {
        if (alts[i].poll_fn)() {
            if let Some(value) = (alts[i].do_fn)() {
                if cas(&flag, OpState::Waiting, OpState::Synched) {
                    let _ = tx.send(value);
                }
                return;
            }
        }
    }

    // Block path: use the same Notify+slot pattern as perform_choice.
    let result_slot: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
    let notify = Arc::new(Notify::new());

    for alt in &alts {
        let (alt_tx, alt_rx) = oneshot::channel::<Value>();
        let slot = result_slot.clone();
        let notify_clone = notify.clone();
        tokio::spawn(async move {
            if let Ok(value) = alt_rx.await {
                let mut guard = slot.lock().await;
                if guard.is_none() {
                    *guard = Some(value);
                }
                notify_clone.notify_one();
            }
        });
        (alt.block_fn)(flag.clone(), alt_tx);
    }

    let notify_final = notify;
    let slot_final = result_slot;
    tokio::spawn(async move {
        notify_final.notified().await;
        if let Some(value) = slot_final.lock().await.take() {
            let _ = tx.send(value);
        }
    });
}

#[bridge(name = "%guard-evt", lib = "(cml bridge)")]
pub async fn guard_evt_bridge(thunk: Procedure) -> Result<Vec<Value>, Exception> {
    let poll_fn: PollFn = Arc::new(|| false);
    let do_fn: DoFn = Arc::new(|| None);

    let (abort_slot, cancel_fn) = make_abort_cancel();
    let thunk_clone = thunk.clone();
    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        let thunk = thunk_clone.clone();
        let handle = tokio::spawn(async move {
            let result = thunk.call(&[], &mut ContBarrier::new()).await;
            if let Ok(results) = result {
                let evt_val = match results.into_iter().next() {
                    Some(v) => v,
                    None => return,
                };
                if let Ok(inner) = evt_val.try_to_rust_type::<BaseEvent>() {
                    if (inner.poll_fn)() {
                        if let Some(value) = (inner.do_fn)() {
                            if cas(&flag, OpState::Waiting, OpState::Synched) {
                                let _ = tx.send(value);
                            }
                            return;
                        }
                    }
                    (inner.block_fn)(flag, tx);
                } else if let Ok(choice) = evt_val.try_to_rust_type::<ChoiceEvent>() {
                    guard_sync_choice(&choice, flag, tx);
                }
            }
        });
        *abort_slot.lock().unwrap() = Some(handle.abort_handle());
    });

    let event = BaseEvent {
        poll_fn,
        do_fn,
        block_fn,
        cancel_fn,
        wrap_fns: Vec::new(),
    };
    Ok(vec![Value::from_rust_type(event)])
}

#[bridge(name = "%always-evt", lib = "(cml bridge)")]
pub async fn always_evt_bridge(val: &Value) -> Result<Vec<Value>, Exception> {
    let poll_fn: PollFn = Arc::new(|| true);

    let v = val.clone();
    let do_fn: DoFn = Arc::new(move || Some(v.clone()));

    let v = val.clone();
    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        if cas(&flag, OpState::Waiting, OpState::Synched) {
            let _ = tx.send(v.clone());
        }
    });

    let cancel_fn: CancelFn = Arc::new(|| {});

    Ok(vec![Value::from_rust_type(BaseEvent {
        poll_fn,
        do_fn,
        block_fn,
        cancel_fn,
        wrap_fns: Vec::new(),
    })])
}

#[bridge(name = "%never-evt", lib = "(cml bridge)")]
pub async fn never_evt_bridge() -> Result<Vec<Value>, Exception> {
    let poll_fn: PollFn = Arc::new(|| false);
    let do_fn: DoFn = Arc::new(|| None);
    let block_fn: BlockFn = Arc::new(|_flag: Flag, _tx: ResumeTx| {});
    let cancel_fn: CancelFn = Arc::new(|| {});

    Ok(vec![Value::from_rust_type(BaseEvent {
        poll_fn,
        do_fn,
        block_fn,
        cancel_fn,
        wrap_fns: Vec::new(),
    })])
}
