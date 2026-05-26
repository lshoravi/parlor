use scheme_rs::exceptions::Exception;
use scheme_rs::proc::Procedure;
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;

use crate::event::Event;

#[bridge(name = "%make-custom-event", lib = "(cml bridge)")]
pub async fn make_custom_event(thunk: Procedure) -> Result<Vec<Value>, Exception> {
    let event = Event::Custom { thunk };
    Ok(vec![Value::from_rust_type(event)])
}
