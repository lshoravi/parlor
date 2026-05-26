use std::sync::Arc;

use scheme_rs::exceptions::Exception;
use scheme_rs::gc::{OpaqueGcPtr, Trace};
use scheme_rs::records::{RecordTypeDescriptor, SchemeCompatible, rtd};
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;
use tokio::sync::{mpsc, oneshot};

use crate::event::Event;

pub struct Envelope {
    pub msg: Value,
    pub ack: Option<oneshot::Sender<()>>,
}

#[derive(Clone)]
pub struct CmlChannel {
    pub sender: Arc<mpsc::Sender<Envelope>>,
    pub receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<Envelope>>>,
    pub is_rendezvous: bool,
}

impl CmlChannel {
    pub fn new_rendezvous() -> Self {
        let (tx, rx) = mpsc::channel::<Envelope>(1);
        Self {
            sender: Arc::new(tx),
            receiver: Arc::new(tokio::sync::Mutex::new(rx)),
            is_rendezvous: true,
        }
    }

    pub fn new_buffered(capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel::<Envelope>(capacity.max(1));
        Self {
            sender: Arc::new(tx),
            receiver: Arc::new(tokio::sync::Mutex::new(rx)),
            is_rendezvous: false,
        }
    }

    pub fn into_value(self) -> Value {
        Value::from_rust_type(self)
    }
}

impl std::fmt::Debug for CmlChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CmlChannel")
            .field("is_rendezvous", &self.is_rendezvous)
            .finish_non_exhaustive()
    }
}

unsafe impl Trace for CmlChannel {
    unsafe fn visit_children(&self, _visitor: &mut dyn FnMut(OpaqueGcPtr)) {}

    unsafe fn finalize(&mut self) {
        unsafe {
            std::ptr::drop_in_place(self as *mut Self);
        }
    }
}

impl SchemeCompatible for CmlChannel {
    fn rtd() -> Arc<RecordTypeDescriptor> {
        rtd!(
            name: "cml-channel",
            opaque: true,
            sealed: true,
        )
    }
}

#[bridge(name = "%make-rendezvous-channel", lib = "(cml channels bridge)")]
pub async fn make_rendezvous_channel() -> Result<Vec<Value>, Exception> {
    let (tx, rx) = mpsc::channel(1);
    let ch = CmlChannel {
        sender: Arc::new(tx),
        receiver: Arc::new(tokio::sync::Mutex::new(rx)),
        is_rendezvous: true,
    };
    Ok(vec![Value::from_rust_type(ch)])
}

#[bridge(name = "%make-buffered-channel", lib = "(cml channels bridge)")]
pub async fn make_buffered_channel(capacity: usize) -> Result<Vec<Value>, Exception> {
    if capacity == 0 {
        return Err(Exception::error("buffered channel capacity must be > 0"));
    }
    let (tx, rx) = mpsc::channel(capacity);
    let ch = CmlChannel {
        sender: Arc::new(tx),
        receiver: Arc::new(tokio::sync::Mutex::new(rx)),
        is_rendezvous: false,
    };
    Ok(vec![Value::from_rust_type(ch)])
}

#[bridge(name = "%send-evt", lib = "(cml channels bridge)")]
pub async fn send_evt(ch_val: &Value, msg: &Value) -> Result<Vec<Value>, Exception> {
    let channel = ch_val.try_to_rust_type::<CmlChannel>()?;
    let event = Event::ChannelSend {
        channel: (*channel).clone(),
        msg: msg.clone(),
    };
    Ok(vec![Value::from_rust_type(event)])
}

#[bridge(name = "%recv-evt", lib = "(cml channels bridge)")]
pub async fn recv_evt(ch_val: &Value) -> Result<Vec<Value>, Exception> {
    let channel = ch_val.try_to_rust_type::<CmlChannel>()?;
    let event = Event::ChannelRecv {
        channel: (*channel).clone(),
    };
    Ok(vec![Value::from_rust_type(event)])
}
