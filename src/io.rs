use std::sync::Arc;

use scheme_rs::exceptions::Exception;
use scheme_rs::lists::Pair;
use scheme_rs::ports::{BufferMode, Port};
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;
use tokio::net::TcpListener;
use tokio::task::AbortHandle;

use crate::event::{BaseEvent, BlockFn, CancelFn, Flag, OpState, ResumeTx, TryFn, cas};

fn accept_result(socket: tokio::net::TcpStream, addr: std::net::SocketAddr) -> Value {
    let port = Value::from(Port::new(addr.to_string(), socket, BufferMode::Block, None));
    let addr_val = Value::from(addr.to_string());
    Value::from(Pair::immutable(port, addr_val))
}

#[bridge(name = "%accept-evt", lib = "(cml io bridge)")]
pub async fn accept_evt_bridge(listener_val: &Value) -> Result<Vec<Value>, Exception> {
    let listener = listener_val.try_to_rust_type::<Arc<TcpListener>>()?;
    let listener = (*listener).clone();

    let try_fn: TryFn = Arc::new(|| None);

    let abort_slot: Arc<std::sync::Mutex<Option<AbortHandle>>> =
        Arc::new(std::sync::Mutex::new(None));

    let slot_for_block = abort_slot.clone();
    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        let listener = listener.clone();
        let handle = tokio::spawn(async move {
            if let Ok((socket, addr)) = listener.accept().await {
                if cas(&flag, OpState::Waiting, OpState::Synched) {
                    let _ = tx.send(accept_result(socket, addr));
                }
            }
        });
        *slot_for_block.lock().unwrap() = Some(handle.abort_handle());
    });

    let cancel_fn: CancelFn = Arc::new(move || {
        if let Some(h) = abort_slot.lock().unwrap().take() {
            h.abort();
        }
    });

    let event = BaseEvent {
        try_fn,
        block_fn,
        cancel_fn,
        wrap_fns: Vec::new(),
    };
    Ok(vec![Value::from_rust_type(event)])
}

#[bridge(name = "%readable-evt", lib = "(cml io bridge)")]
pub async fn readable_evt_bridge(port_val: &Value) -> Result<Vec<Value>, Exception> {
    let port: Port = port_val.clone().try_into().map_err(|_| {
        Exception::error("readable-evt: expected a port")
    })?;
    let result_val = port_val.clone();

    let poll_port = port.clone();
    let try_fn: TryFn = Arc::new(move || {
        if poll_port.poll_read_ready() {
            Some(result_val.clone())
        } else {
            None
        }
    });

    let abort_slot: Arc<std::sync::Mutex<Option<AbortHandle>>> =
        Arc::new(std::sync::Mutex::new(None));

    let wait_val = port_val.clone();
    let slot_for_block = abort_slot.clone();
    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        let port = port.clone();
        let val = wait_val.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::task::yield_now().await;
                if port.poll_read_ready() {
                    if cas(&flag, OpState::Waiting, OpState::Synched) {
                        let _ = tx.send(val);
                    }
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        });
        *slot_for_block.lock().unwrap() = Some(handle.abort_handle());
    });

    let cancel_fn: CancelFn = Arc::new(move || {
        if let Some(h) = abort_slot.lock().unwrap().take() {
            h.abort();
        }
    });

    let event = BaseEvent {
        try_fn,
        block_fn,
        cancel_fn,
        wrap_fns: Vec::new(),
    };
    Ok(vec![Value::from_rust_type(event)])
}

#[bridge(name = "%writable-evt", lib = "(cml io bridge)")]
pub async fn writable_evt_bridge(port_val: &Value) -> Result<Vec<Value>, Exception> {
    let port: Port = port_val.clone().try_into().map_err(|_| {
        Exception::error("writable-evt: expected a port")
    })?;
    let result_val = port_val.clone();

    let poll_port = port.clone();
    let try_fn: TryFn = Arc::new(move || {
        if poll_port.poll_write_ready() {
            Some(result_val.clone())
        } else {
            None
        }
    });

    let abort_slot: Arc<std::sync::Mutex<Option<AbortHandle>>> =
        Arc::new(std::sync::Mutex::new(None));

    let wait_val = port_val.clone();
    let slot_for_block = abort_slot.clone();
    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        let port = port.clone();
        let val = wait_val.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::task::yield_now().await;
                if port.poll_write_ready() {
                    if cas(&flag, OpState::Waiting, OpState::Synched) {
                        let _ = tx.send(val);
                    }
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        });
        *slot_for_block.lock().unwrap() = Some(handle.abort_handle());
    });

    let cancel_fn: CancelFn = Arc::new(move || {
        if let Some(h) = abort_slot.lock().unwrap().take() {
            h.abort();
        }
    });

    let event = BaseEvent {
        try_fn,
        block_fn,
        cancel_fn,
        wrap_fns: Vec::new(),
    };
    Ok(vec![Value::from_rust_type(event)])
}
