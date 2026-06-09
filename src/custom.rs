use std::sync::Arc;

use scheme_rs::exceptions::Exception;
use scheme_rs::proc::{ContBarrier, Procedure};
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;

use crate::event::{BaseEvent, BlockFn, DoFn, Flag, OpState, PollFn, ResumeTx, cas, make_abort_cancel};

#[bridge(name = "%make-custom-event", lib = "(cml bridge)")]
pub async fn make_custom_event(thunk: Procedure) -> Result<Vec<Value>, Exception> {
    let poll_fn: PollFn = Arc::new(|| false);
    let do_fn: DoFn = Arc::new(|| None);

    let (abort_slot, cancel_fn) = make_abort_cancel();
    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        let thunk = thunk.clone();
        let handle = tokio::spawn(async move {
            if let Ok(results) = thunk.call(&[], &mut ContBarrier::new()).await
                && cas(&flag, OpState::Waiting, OpState::Synched) {
                    let value = results.into_iter().next().unwrap_or(Value::from(false));
                    let _ = tx.send(value);
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
