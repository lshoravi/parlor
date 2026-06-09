use std::sync::Arc;

use scheme_rs::exceptions::Exception;
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;
use tokio::task::AbortHandle;

use crate::event::{BaseEvent, BlockFn, CancelFn, DoFn, Flag, OpState, PollFn, ResumeTx, cas};

fn make_timer_event(duration: std::time::Duration) -> BaseEvent {
    let is_zero = duration.is_zero();
    let poll_fn: PollFn = Arc::new(move || is_zero);
    let do_fn: DoFn = if duration.is_zero() {
        Arc::new(|| Some(Value::from(false)))
    } else {
        Arc::new(|| None)
    };

    let abort_slot: Arc<std::sync::Mutex<Option<AbortHandle>>> =
        Arc::new(std::sync::Mutex::new(None));

    let slot_for_block = abort_slot.clone();
    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        let handle = tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            if cas(&flag, OpState::Waiting, OpState::Synched) {
                let _ = tx.send(Value::from(false));
            }
        });
        *slot_for_block.lock().unwrap() = Some(handle.abort_handle());
    });

    let cancel_fn: CancelFn = Arc::new(move || {
        if let Some(h) = abort_slot.lock().unwrap().take() {
            h.abort();
        }
    });

    BaseEvent {
        poll_fn,
        do_fn,
        block_fn,
        cancel_fn,
        wrap_fns: Vec::new(),
    }
}

#[bridge(name = "%sleep-evt", lib = "(cml timers bridge)")]
pub async fn sleep_evt(seconds: f64) -> Result<Vec<Value>, Exception> {
    let nanos = (seconds * 1_000_000_000.0) as u64;
    let duration = std::time::Duration::from_nanos(nanos);
    Ok(vec![Value::from_rust_type(make_timer_event(duration))])
}

#[bridge(name = "%timer-operation", lib = "(cml timers bridge)")]
pub async fn timer_operation(expiry: f64) -> Result<Vec<Value>, Exception> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    let remaining = (expiry - now).max(0.0);
    let nanos = (remaining * 1_000_000_000.0) as u64;
    let duration = std::time::Duration::from_nanos(nanos);
    Ok(vec![Value::from_rust_type(make_timer_event(duration))])
}
