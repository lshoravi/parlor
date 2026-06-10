use std::sync::Arc;

use futures::future::{BoxFuture, Shared};
use scheme_rs::exceptions::Exception;
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;
use tokio::sync::watch;

use crate::event::{BaseEvent, BlockFn, CancelFn, DoFn, Flag, OpState, PollFn, ResumeTx, cas};

type Future = Shared<BoxFuture<'static, Result<Vec<Value>, Exception>>>;

#[bridge(name = "%join-evt", lib = "(cml spawn bridge)")]
pub async fn join_evt_bridge(future_val: &Value) -> Result<Vec<Value>, Exception> {
    let future = future_val.try_to_rust_type::<Future>()?;
    let (watch_tx, watch_rx) = watch::channel(None::<Value>);

    let fut = (*future).clone();
    tokio::spawn(async move {
        if let Ok(results) = fut.await {
            let value = results.into_iter().next().unwrap_or(Value::from(false));
            let _ = watch_tx.send(Some(value));
        }
    });

    let rx = watch_rx;

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
