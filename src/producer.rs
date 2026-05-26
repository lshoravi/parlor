use std::sync::Arc;

use scheme_rs::exceptions::Exception;
use scheme_rs::value::Value;
use tokio::sync::mpsc;

use crate::channels::{CmlChannel, Envelope};

/// Rust-side handle for sending into a CML channel.
pub struct CmlProducer {
    sender: Arc<mpsc::Sender<Envelope>>,
}

impl CmlProducer {
    pub fn from_channel_value(val: &Value) -> Result<Self, Exception> {
        let ch = val.try_to_rust_type::<CmlChannel>()?;
        Ok(Self {
            sender: ch.sender.clone(),
        })
    }

    pub fn try_send(&self, val: Value) -> Result<(), Exception> {
        self.sender
            .try_send(Envelope { msg: val, ack: None })
            .map_err(|_| Exception::error("channel full or closed"))
    }

    pub async fn send(&self, val: Value) -> Result<(), Exception> {
        self.sender
            .send(Envelope { msg: val, ack: None })
            .await
            .map_err(|_| Exception::error("channel closed"))
    }
}

/// Rust-side handle for receiving from a CML channel.
pub struct CmlConsumer {
    receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<Envelope>>>,
}

impl CmlConsumer {
    pub fn from_channel_value(val: &Value) -> Result<Self, Exception> {
        let ch = val.try_to_rust_type::<CmlChannel>()?;
        Ok(Self {
            receiver: ch.receiver.clone(),
        })
    }

    pub async fn recv(&self) -> Result<Value, Exception> {
        let mut rx = self.receiver.lock().await;
        let envelope = rx
            .recv()
            .await
            .ok_or_else(|| Exception::error("channel closed"))?;
        if let Some(ack) = envelope.ack {
            let _ = ack.send(());
        }
        Ok(envelope.msg)
    }
}
