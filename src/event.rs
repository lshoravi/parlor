use std::sync::Arc;
use std::time::Duration;

use scheme_rs::exceptions::Exception;
use scheme_rs::gc::Trace;
use scheme_rs::proc::{ContBarrier, Procedure};
use scheme_rs::records::{RecordTypeDescriptor, SchemeCompatible, rtd};
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;

use crate::channels::{CmlChannel, Envelope};

#[derive(Clone, Debug, Trace)]
pub enum Event {
    Timer { nanos: u64 },
    Custom { thunk: Procedure },
    Wrapped { inner: Box<Event>, transform: Procedure },
    Choice { alternatives: Vec<Event> },
    Guard { thunk: Procedure },
    ChannelSend { channel: CmlChannel, msg: Value },
    ChannelRecv { channel: CmlChannel },
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
