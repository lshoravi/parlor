use scheme_rs::exceptions::Exception;
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;

use crate::event::Event;

#[bridge(name = "%sleep-evt", lib = "(cml timers bridge)")]
pub async fn sleep_evt(seconds: f64) -> Result<Vec<Value>, Exception> {
    let nanos = (seconds * 1_000_000_000.0) as u64;
    let event = Event::Timer { nanos };
    Ok(vec![Value::from_rust_type(event)])
}
