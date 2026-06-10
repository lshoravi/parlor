use std::sync::Arc;

use scheme_rs::exceptions::Exception;
use scheme_rs::gc::{OpaqueGcPtr, Trace};
use scheme_rs::proc::{ContBarrier, Procedure};
use scheme_rs::records::{RecordTypeDescriptor, SchemeCompatible, rtd};
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;
use tokio::sync::watch;

use crate::event::{BaseEvent, BlockFn, CancelFn, DoFn, Flag, OpState, PollFn, ResumeTx, cas};

#[derive(Clone)]
pub struct TaskHandle {
    rx: watch::Receiver<Option<Value>>,
}

impl std::fmt::Debug for TaskHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskHandle").finish_non_exhaustive()
    }
}

unsafe impl Trace for TaskHandle {
    unsafe fn visit_children(&self, _visitor: &mut dyn FnMut(OpaqueGcPtr)) {}
    unsafe fn finalize(&mut self) {
        unsafe { std::ptr::drop_in_place(self as *mut Self) }
    }
}

impl SchemeCompatible for TaskHandle {
    fn rtd() -> Arc<RecordTypeDescriptor> {
        rtd!(name: "cml-task-handle", opaque: true, sealed: true)
    }
}

#[bridge(name = "%cml-spawn", lib = "(cml spawn bridge)")]
pub async fn cml_spawn(thunk: Procedure) -> Result<Vec<Value>, Exception> {
    let (tx, rx) = watch::channel(None);
    tokio::spawn(async move {
        if let Ok(results) = thunk.call(&[], &mut ContBarrier::new()).await {
            let value = results.into_iter().next().unwrap_or(Value::from(false));
            let _ = tx.send(Some(value));
        }
    });
    Ok(vec![Value::from_rust_type(TaskHandle { rx })])
}

#[bridge(name = "%join-evt", lib = "(cml spawn bridge)")]
pub async fn join_evt_bridge(handle_val: &Value) -> Result<Vec<Value>, Exception> {
    let handle = handle_val.try_to_rust_type::<TaskHandle>()?;
    let rx = handle.rx.clone();

    let rx_poll = rx.clone();
    let poll_fn: PollFn = Arc::new(move || rx_poll.borrow().is_some());

    let rx_do = rx.clone();
    let do_fn: DoFn = Arc::new(move || rx_do.borrow().clone());

    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        let mut rx = rx.clone();
        let handle = tokio::spawn(async move {
            loop {
                if let Some(value) = rx.borrow_and_update().clone() {
                    if cas(&flag, OpState::Waiting, OpState::Synched) {
                        let _ = tx.send(value);
                    }
                    return;
                }
                if rx.changed().await.is_err() {
                    return;
                }
            }
        });
        Some(handle.abort_handle())
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
