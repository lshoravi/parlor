use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use scheme_rs::exceptions::Exception;
use scheme_rs::gc::{OpaqueGcPtr, Trace};
use scheme_rs::proc::{ContBarrier, Procedure};
use scheme_rs::records::{RecordTypeDescriptor, SchemeCompatible, rtd};
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;
use tokio::sync::Notify;

use crate::channels::{CmlChannel, Envelope};

#[derive(Clone, Debug)]
pub enum Event {
    Timer { nanos: u64 },
    Custom { thunk: Procedure },
    Wrapped { inner: Box<Event>, transform: Procedure },
    Choice { alternatives: Vec<Event> },
    Guard { thunk: Procedure },
    ChannelSend { channel: CmlChannel, msg: Value },
    ChannelRecv { channel: CmlChannel },
    ConditionWait { notify: Arc<Notify>, signalled: Arc<AtomicBool> },
    NotifierWait { notify: Arc<Notify> },
}

unsafe impl Trace for Event {
    unsafe fn visit_children(&self, visitor: &mut dyn FnMut(OpaqueGcPtr)) {
        match self {
            Event::Timer { .. } => {}
            Event::Custom { thunk } => unsafe { thunk.visit_children(visitor) },
            Event::Wrapped { inner, transform } => unsafe {
                inner.visit_children(visitor);
                transform.visit_children(visitor);
            },
            Event::Choice { alternatives } => {
                for alt in alternatives {
                    unsafe { alt.visit_children(visitor) }
                }
            }
            Event::Guard { thunk } => unsafe { thunk.visit_children(visitor) },
            Event::ChannelSend { channel, msg } => unsafe {
                channel.visit_children(visitor);
                msg.visit_children(visitor);
            },
            Event::ChannelRecv { channel } => unsafe { channel.visit_children(visitor) },
            Event::ConditionWait { .. } => {}
            Event::NotifierWait { .. } => {}
        }
    }

    unsafe fn finalize(&mut self) {
        unsafe { std::ptr::drop_in_place(self as *mut Self) }
    }
}

impl SchemeCompatible for Event {
    fn rtd() -> Arc<RecordTypeDescriptor> {
        rtd!(
            name: "cml-event",
            opaque: true,
            sealed: true,
        )
    }
}

async fn perform(event: &Event) -> Result<Vec<Value>, Exception> {
    match event {
        Event::Timer { nanos } => {
            let duration = Duration::from_nanos(*nanos);
            if !duration.is_zero() {
                tokio::time::sleep(duration).await;
            }
            Ok(vec![Value::from(false)])
        }
        Event::Wrapped { inner, transform } => {
            let result = Box::pin(perform(inner)).await?;
            transform.call(&result, &mut ContBarrier::new()).await
        }
        Event::Guard { thunk } => {
            let result = thunk.call(&[], &mut ContBarrier::new()).await?;
            if result.is_empty() {
                return Err(Exception::error("guard returned no value"));
            }
            let inner = result[0].try_to_rust_type::<Event>()?;
            Box::pin(perform(&inner)).await
        }
        Event::Custom { thunk } => {
            thunk.call(&[], &mut ContBarrier::new()).await
        }
        Event::Choice { .. } => {
            Err(Exception::error("choose: not yet implemented"))
        }
        Event::ChannelSend { channel, msg } => {
            if channel.is_rendezvous {
                let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
                channel
                    .sender
                    .send(Envelope {
                        msg: msg.clone(),
                        ack: Some(ack_tx),
                    })
                    .await
                    .map_err(|_| Exception::error("channel closed"))?;
                ack_rx
                    .await
                    .map_err(|_| Exception::error("receiver dropped"))?;
            } else {
                channel
                    .sender
                    .send(Envelope {
                        msg: msg.clone(),
                        ack: None,
                    })
                    .await
                    .map_err(|_| Exception::error("channel closed"))?;
            }
            Ok(vec![Value::from(false)])
        }
        Event::ChannelRecv { channel } => {
            let mut rx = channel.receiver.lock().await;
            let envelope = rx
                .recv()
                .await
                .ok_or_else(|| Exception::error("channel closed"))?;
            if let Some(ack) = envelope.ack {
                let _ = ack.send(());
            }
            Ok(vec![envelope.msg])
        }
        Event::ConditionWait { notify, signalled } => {
            if signalled.load(Ordering::SeqCst) {
                return Ok(vec![Value::from(true)]);
            }
            notify.notified().await;
            Ok(vec![Value::from(true)])
        }
        Event::NotifierWait { notify } => {
            notify.notified().await;
            Ok(vec![Value::from(true)])
        }
    }
}

#[bridge(name = "%sync", lib = "(cml bridge)")]
pub async fn sync_bridge(evt_val: &Value) -> Result<Vec<Value>, Exception> {
    let event = evt_val.try_to_rust_type::<Event>()?;
    perform(&event).await
}

#[bridge(name = "%wrap", lib = "(cml bridge)")]
pub async fn wrap_bridge(evt_val: &Value, transform: Procedure) -> Result<Vec<Value>, Exception> {
    let event = evt_val.try_to_rust_type::<Event>()?;
    let wrapped = Event::Wrapped {
        inner: Box::new((*event).clone()),
        transform,
    };
    Ok(vec![Value::from_rust_type(wrapped)])
}

#[bridge(name = "%choose", lib = "(cml bridge)")]
pub async fn choose_bridge(evts: &[Value]) -> Result<Vec<Value>, Exception> {
    let mut alternatives = Vec::new();
    for v in evts {
        let e = v.try_to_rust_type::<Event>()?;
        alternatives.push((*e).clone());
    }
    let choice = Event::Choice { alternatives };
    Ok(vec![Value::from_rust_type(choice)])
}

#[bridge(name = "%guard", lib = "(cml bridge)")]
pub async fn guard_bridge(thunk: Procedure) -> Result<Vec<Value>, Exception> {
    Ok(vec![Value::from_rust_type(Event::Guard { thunk })])
}
