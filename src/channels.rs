use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use imbl::Vector;
use scheme_rs::exceptions::Exception;
use scheme_rs::gc::{OpaqueGcPtr, Trace};
use scheme_rs::records::{RecordTypeDescriptor, SchemeCompatible, rtd};
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;

use crate::event::{
    BaseEvent, BlockFn, CancelFn, Flag, OpState, ResumeTx, TryFn, cas, flag_state,
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

const GC_INTERVAL: usize = 64;

pub struct ChannelInner {
    putq: ArcSwap<Vector<Arc<SendWaiter>>>,
    getq: ArcSwap<Vector<Arc<RecvWaiter>>>,
    putq_gc_counter: AtomicUsize,
    getq_gc_counter: AtomicUsize,
    pub capacity: Option<usize>,
    pub buffer: Option<ArcSwap<Vector<Value>>>,
}

impl ChannelInner {
    fn gc_senders(&self) {
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

    fn gc_receivers(&self) {
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

#[derive(Clone)]
pub struct CmlChannel {
    pub inner: Arc<ChannelInner>,
}

impl CmlChannel {
    pub fn new_rendezvous() -> Self {
        Self {
            inner: Arc::new(ChannelInner {
                putq: ArcSwap::from_pointee(Vector::new()),
                getq: ArcSwap::from_pointee(Vector::new()),
                putq_gc_counter: AtomicUsize::new(GC_INTERVAL),
                getq_gc_counter: AtomicUsize::new(GC_INTERVAL),
                capacity: None,
                buffer: None,
            }),
        }
    }

    pub fn new_buffered(capacity: usize) -> Self {
        Self {
            inner: Arc::new(ChannelInner {
                putq: ArcSwap::from_pointee(Vector::new()),
                getq: ArcSwap::from_pointee(Vector::new()),
                putq_gc_counter: AtomicUsize::new(GC_INTERVAL),
                getq_gc_counter: AtomicUsize::new(GC_INTERVAL),
                capacity: Some(capacity),
                buffer: Some(ArcSwap::from_pointee(Vector::new())),
            }),
        }
    }
}

impl std::fmt::Debug for CmlChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CmlChannel")
            .field("capacity", &self.inner.capacity)
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

pub fn recv_event(channel: CmlChannel) -> BaseEvent {
    let ch = channel.clone();
    let try_fn: TryFn = Arc::new(move || {
        if let Some(ref buf) = ch.inner.buffer {
            let popped: Arc<std::sync::Mutex<Option<Value>>> =
                Arc::new(std::sync::Mutex::new(None));
            let popped_clone = popped.clone();
            buf.rcu(move |b| {
                if b.is_empty() {
                    return Arc::clone(b);
                }
                let mut new_buf = (**b).clone();
                let value = new_buf.pop_front().unwrap();
                *popped_clone.lock().unwrap() = Some(value);
                Arc::new(new_buf)
            });
            if let Some(value) = popped.lock().unwrap().take() {
                return Some(value);
            }
        }

        let putq = ch.inner.putq.load();
        for sender in putq.iter() {
            if cas(&sender.flag, OpState::Waiting, OpState::Synched) {
                let message = sender.message.clone();
                if let Some(tx) = sender.tx.lock().unwrap().take() {
                    let _ = tx.send(Value::from(false));
                }
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

        let waiter_for_queue = waiter.clone();
        ch.inner.getq.rcu(move |q| {
            let mut q = (**q).clone();
            q.push_back(waiter_for_queue.clone());
            Arc::new(q)
        });

        ch.inner.gc_receivers();

        if let Some(ref buf) = ch.inner.buffer {
            let guard = buf.load();
            if !guard.is_empty() {
                if cas(&flag, OpState::Waiting, OpState::Synched) {
                    let popped: Arc<std::sync::Mutex<Option<Value>>> =
                        Arc::new(std::sync::Mutex::new(None));
                    let popped_clone = popped.clone();
                    buf.rcu(move |b| {
                        if b.is_empty() {
                            return Arc::clone(b);
                        }
                        let mut new_buf = (**b).clone();
                        let value = new_buf.pop_front().unwrap();
                        *popped_clone.lock().unwrap() = Some(value);
                        Arc::new(new_buf)
                    });
                    if let Some(value) = popped.lock().unwrap().take() {
                        if let Some(rtx) = waiter.tx.lock().unwrap().take() {
                            let _ = rtx.send(value);
                        }
                    } else {
                        flag.store(OpState::Waiting as u8, Ordering::SeqCst);
                    }
                    return;
                }
            }
        }

        let putq = ch.inner.putq.load();
        for sender in putq.iter() {
            if flag_state(&sender.flag) == OpState::Synched {
                continue;
            }

            if !cas(&flag, OpState::Waiting, OpState::Claimed) {
                return;
            }

            match sender.flag.compare_exchange(
                OpState::Waiting as u8,
                OpState::Synched as u8,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    flag.store(OpState::Synched as u8, Ordering::SeqCst);
                    let message = sender.message.clone();
                    if let Some(stx) = sender.tx.lock().unwrap().take() {
                        let _ = stx.send(Value::from(false));
                    }
                    if let Some(rtx) = waiter.tx.lock().unwrap().take() {
                        let _ = rtx.send(message);
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

pub fn send_event(channel: CmlChannel, msg: Value) -> BaseEvent {
    let ch = channel.clone();
    let msg_clone = msg.clone();
    let try_fn: TryFn = Arc::new(move || {
        let getq = ch.inner.getq.load();
        for receiver in getq.iter() {
            if cas(&receiver.flag, OpState::Waiting, OpState::Synched) {
                if let Some(tx) = receiver.tx.lock().unwrap().take() {
                    let _ = tx.send(msg_clone.clone());
                }
                return Some(Value::from(false));
            }
        }

        if let (Some(buf), Some(cap)) = (&ch.inner.buffer, ch.inner.capacity) {
            let sent = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let sent_clone = sent.clone();
            let msg_for_buf = msg_clone.clone();
            buf.rcu(move |b| {
                if b.len() < cap {
                    let mut new_buf = (**b).clone();
                    new_buf.push_back(msg_for_buf.clone());
                    sent_clone.store(true, Ordering::Relaxed);
                    Arc::new(new_buf)
                } else {
                    sent_clone.store(false, Ordering::Relaxed);
                    Arc::clone(b)
                }
            });
            if sent.load(Ordering::Relaxed) {
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

        let waiter_for_queue = waiter.clone();
        ch.inner.putq.rcu(move |q| {
            let mut q = (**q).clone();
            q.push_back(waiter_for_queue.clone());
            Arc::new(q)
        });

        ch.inner.gc_senders();

        let getq = ch.inner.getq.load();
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
                        let _ = rtx.send(waiter.message.clone());
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

#[bridge(name = "%make-rendezvous-channel", lib = "(cml channels bridge)")]
pub async fn make_rendezvous_channel() -> Result<Vec<Value>, Exception> {
    Ok(vec![Value::from_rust_type(CmlChannel::new_rendezvous())])
}

#[bridge(name = "%make-buffered-channel", lib = "(cml channels bridge)")]
pub async fn make_buffered_channel(capacity: usize) -> Result<Vec<Value>, Exception> {
    if capacity == 0 {
        return Err(Exception::error("buffered channel capacity must be > 0"));
    }
    Ok(vec![Value::from_rust_type(CmlChannel::new_buffered(
        capacity,
    ))])
}

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
