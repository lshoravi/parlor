use std::sync::Arc;

use scheme_rs::exceptions::Exception;
use scheme_rs::lists::Pair;
use scheme_rs::ports::{BufferMode, Port};
use scheme_rs::registry::bridge;
use scheme_rs::strings::WideString;
use scheme_rs::value::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::AbortHandle;

use crate::event::{BaseEvent, BlockFn, CancelFn, Flag, OpState, ResumeTx, TryFn, cas};

fn accept_result(socket: TcpStream, addr: std::net::SocketAddr) -> Value {
    let port = Value::from(Port::new(addr.to_string(), socket, BufferMode::Block, None));
    let addr_val = Value::from(addr.to_string());
    Value::from(Pair::immutable(port, addr_val))
}

fn make_abort_cancel() -> (Arc<std::sync::Mutex<Option<AbortHandle>>>, CancelFn) {
    let slot: Arc<std::sync::Mutex<Option<AbortHandle>>> =
        Arc::new(std::sync::Mutex::new(None));
    let slot_for_cancel = slot.clone();
    let cancel_fn: CancelFn = Arc::new(move || {
        if let Some(h) = slot_for_cancel.lock().unwrap().take() {
            h.abort();
        }
    });
    (slot, cancel_fn)
}

// --- Networking helpers ---

#[bridge(name = "%connect-tcp", lib = "(cml io bridge)")]
pub async fn connect_tcp(addr: &Value) -> Result<Vec<Value>, Exception> {
    let addr: WideString = addr.clone().try_into()?;
    let stream = TcpStream::connect(&addr.to_string())
        .await
        .map_err(|e| Exception::error(format!("connect-tcp: {e}")))?;
    let peer = stream.peer_addr().map(|a| a.to_string()).unwrap_or_default();
    let port = Value::from(Port::new(peer, stream, BufferMode::Block, None));
    Ok(vec![port])
}

#[bridge(name = "%listener-address", lib = "(cml io bridge)")]
pub async fn listener_address(listener_val: &Value) -> Result<Vec<Value>, Exception> {
    let listener = listener_val.try_to_rust_type::<Arc<TcpListener>>()?;
    let addr = listener
        .local_addr()
        .map_err(|e| Exception::error(format!("listener-address: {e}")))?;
    Ok(vec![Value::from(addr.to_string())])
}

// --- CML events ---

#[bridge(name = "%accept-evt", lib = "(cml io bridge)")]
pub async fn accept_evt_bridge(listener_val: &Value) -> Result<Vec<Value>, Exception> {
    let listener = listener_val.try_to_rust_type::<Arc<TcpListener>>()?;
    let listener = (*listener).clone();

    let try_fn: TryFn = Arc::new(|| None);

    let (abort_slot, cancel_fn) = make_abort_cancel();
    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        let listener = listener.clone();
        let handle = tokio::spawn(async move {
            if let Ok((socket, addr)) = listener.accept().await {
                if cas(&flag, OpState::Waiting, OpState::Synched) {
                    let _ = tx.send(accept_result(socket, addr));
                }
            }
        });
        *abort_slot.lock().unwrap() = Some(handle.abort_handle());
    });

    Ok(vec![Value::from_rust_type(BaseEvent {
        try_fn,
        block_fn,
        cancel_fn,
        wrap_fns: Vec::new(),
    })])
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

    #[cfg(unix)]
    let (block_fn, cancel_fn) = {
        let fd = port.raw_fd().ok_or_else(|| {
            Exception::error("readable-evt: port has no file descriptor")
        })?;
        make_readiness_block_fn(fd, tokio::io::Interest::READABLE, port_val.clone())
    };

    #[cfg(not(unix))]
    let (block_fn, cancel_fn) =
        make_poll_block_fn(port, true, port_val.clone());

    Ok(vec![Value::from_rust_type(BaseEvent {
        try_fn,
        block_fn,
        cancel_fn,
        wrap_fns: Vec::new(),
    })])
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

    #[cfg(unix)]
    let (block_fn, cancel_fn) = {
        let fd = port.raw_fd().ok_or_else(|| {
            Exception::error("writable-evt: port has no file descriptor")
        })?;
        make_readiness_block_fn(fd, tokio::io::Interest::WRITABLE, port_val.clone())
    };

    #[cfg(not(unix))]
    let (block_fn, cancel_fn) =
        make_poll_block_fn(port, false, port_val.clone());

    Ok(vec![Value::from_rust_type(BaseEvent {
        try_fn,
        block_fn,
        cancel_fn,
        wrap_fns: Vec::new(),
    })])
}

// --- Platform-specific block_fn implementations ---

#[cfg(unix)]
fn make_readiness_block_fn(
    fd: std::os::unix::io::RawFd,
    interest: tokio::io::Interest,
    result_val: Value,
) -> (BlockFn, CancelFn) {
    use std::os::unix::io::AsRawFd;

    struct BorrowedFd(std::os::unix::io::RawFd);
    impl AsRawFd for BorrowedFd {
        fn as_raw_fd(&self) -> std::os::unix::io::RawFd {
            self.0
        }
    }

    let (abort_slot, cancel_fn) = make_abort_cancel();
    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        let val = result_val.clone();
        let handle = tokio::spawn(async move {
            let Ok(async_fd) = tokio::io::unix::AsyncFd::new(BorrowedFd(fd)) else {
                return;
            };
            let _ = async_fd.ready(interest).await;
            if cas(&flag, OpState::Waiting, OpState::Synched) {
                let _ = tx.send(val);
            }
        });
        *abort_slot.lock().unwrap() = Some(handle.abort_handle());
    });

    (block_fn, cancel_fn)
}

#[cfg(not(unix))]
fn make_poll_block_fn(
    port: Port,
    readable: bool,
    result_val: Value,
) -> (BlockFn, CancelFn) {
    let (abort_slot, cancel_fn) = make_abort_cancel();
    let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
        let port = port.clone();
        let val = result_val.clone();
        let handle = tokio::spawn(async move {
            loop {
                let ready = if readable {
                    port.poll_read_ready()
                } else {
                    port.poll_write_ready()
                };
                if ready {
                    if cas(&flag, OpState::Waiting, OpState::Synched) {
                        let _ = tx.send(val);
                    }
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        });
        *abort_slot.lock().unwrap() = Some(handle.abort_handle());
    });

    (block_fn, cancel_fn)
}
