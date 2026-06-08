use std::sync::Arc;

use scheme_rs::exceptions::Exception;
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;

use crate::event::{BaseEvent, BlockFn, CancelFn, Flag, OpState, ResumeTx, TryFn, cas};

#[bridge(name = "%sleep-evt", lib = "(cml timers bridge)")]
pub async fn sleep_evt(seconds: f64) -> Result<Vec<Value>, Exception> {
    let nanos = (seconds * 1_000_000_000.0) as u64;
    let duration = std::time::Duration::from_nanos(nanos);

    let try_fn: TryFn = if duration.is_zero() {
        Arc::new(|| Some(Value::from(false)))
    } else {
        Arc::new(|| None)
    };

    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            if cas(&flag, OpState::Waiting, OpState::Synched) {
                let _ = tx.send(Value::from(false));
            }
        });
    });

    let cancel_fn: CancelFn = Arc::new(|| {});

    let event = BaseEvent {
        try_fn,
        block_fn,
        cancel_fn,
        wrap_fns: Vec::new(),
    };
    Ok(vec![Value::from_rust_type(event)])
}
