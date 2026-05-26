use std::sync::Arc;
use std::time::Duration;

use scheme_rs::exceptions::Exception;
use scheme_rs::gc::Trace;
use scheme_rs::proc::{ContBarrier, Procedure};
use scheme_rs::records::{RecordTypeDescriptor, SchemeCompatible, rtd};
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;

#[derive(Clone, Debug, Trace)]
pub enum Event {
    Timer { nanos: u64 },
    Custom { thunk: Procedure },
    Wrapped { inner: Box<Event>, transform: Procedure },
    Choice { alternatives: Vec<Event> },
    Guard { thunk: Procedure },
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
