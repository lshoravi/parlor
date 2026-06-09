use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use scheme_rs::exceptions::Exception;
use scheme_rs::gc::{OpaqueGcPtr, Trace};
use scheme_rs::records::{RecordTypeDescriptor, SchemeCompatible, rtd};
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;
use tokio::sync::{Notify, Semaphore};

use crate::event::{BaseEvent, BlockFn, DoFn, Flag, OpState, PollFn, ResumeTx, cas, make_abort_cancel};

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
pub struct Notifier {
    pub sem: Arc<Semaphore>,
}

unsafe impl Trace for Notifier {
    unsafe fn visit_children(&self, _visitor: &mut dyn FnMut(OpaqueGcPtr)) {}
    unsafe fn finalize(&mut self) {
        unsafe { std::ptr::drop_in_place(self as *mut Self) }
    }
}

impl SchemeCompatible for Notifier {
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
    let was_first = !cv.signalled.swap(true, Ordering::AcqRel);
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

    let signalled_poll = signalled.clone();
    let poll_fn: PollFn = Arc::new(move || {
        signalled_poll.load(Ordering::Acquire)
    });

    let signalled_do = signalled.clone();
    let do_fn: DoFn = Arc::new(move || {
        if signalled_do.load(Ordering::Acquire) {
            Some(Value::from(true))
        } else {
            None
        }
    });

    let (abort_slot, cancel_fn) = make_abort_cancel();
    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        let signalled = signalled.clone();
        let notify = notify.clone();
        let handle = tokio::spawn(async move {
            if signalled.load(Ordering::Acquire) {
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

#[bridge(name = "%make-notifier", lib = "(cml conditions bridge)")]
pub async fn make_notifier() -> Result<Vec<Value>, Exception> {
    let n = Notifier {
        sem: Arc::new(Semaphore::new(0)),
    };
    Ok(vec![Value::from_rust_type(n)])
}

#[bridge(name = "%notify!", lib = "(cml conditions bridge)")]
pub async fn notify(n_val: &Value) -> Result<Vec<Value>, Exception> {
    let n = n_val.try_to_rust_type::<Notifier>()?;
    n.sem.add_permits(1);
    Ok(vec![])
}

#[bridge(name = "%notify-evt", lib = "(cml conditions bridge)")]
pub async fn notify_evt(n_val: &Value) -> Result<Vec<Value>, Exception> {
    let n = n_val.try_to_rust_type::<Notifier>()?;
    let sem = n.sem.clone();

    let sem_poll = sem.clone();
    let poll_fn: PollFn = Arc::new(move || {
        sem_poll.available_permits() > 0
    });

    let sem_do = sem.clone();
    let do_fn: DoFn = Arc::new(move || match sem_do.try_acquire() {
        Ok(permit) => {
            permit.forget();
            Some(Value::from(true))
        }
        Err(_) => None,
    });

    let (abort_slot_n, cancel_fn) = make_abort_cancel();
    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        let sem = sem.clone();
        let handle = tokio::spawn(async move {
            if let Ok(permit) = sem.acquire().await {
                if cas(&flag, OpState::Waiting, OpState::Synched) {
                    permit.forget();
                    let _ = tx.send(Value::from(true));
                } else {
                    drop(permit);
                    sem.add_permits(1);
                }
            }
        });
        *abort_slot_n.lock().unwrap() = Some(handle.abort_handle());
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
