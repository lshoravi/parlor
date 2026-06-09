use std::sync::Arc;

use scheme_rs::exceptions::Exception;
use scheme_rs::value::Value;

use crate::channels::Channel;

pub struct Producer {
    channel: Channel,
}

impl Producer {
    pub fn from_channel_value(val: &Value) -> Result<Self, Exception> {
        let ch = val.try_to_rust_type::<Channel>()?;
        Ok(Self {
            channel: (*ch).clone(),
        })
    }

    pub fn try_send(&self, val: Value) -> Result<(), Exception> {
        if let (Some(buf), Some(cap)) = (&self.channel.inner.buffer, self.channel.inner.capacity)
        {
            let guard = buf.load();
            if guard.len() < cap {
                let val_for_buf = val;
                buf.rcu(move |b| {
                    let mut b = (**b).clone();
                    b.push_back(val_for_buf.clone());
                    Arc::new(b)
                });
                return Ok(());
            }
        }
        Err(Exception::error("channel full or closed"))
    }

    pub async fn send(&self, val: Value) -> Result<(), Exception> {
        let event = crate::channels::send_event(self.channel.clone(), val);
        crate::event::perform_base(&event).await?;
        Ok(())
    }
}

pub struct Consumer {
    channel: Channel,
}

impl Consumer {
    pub fn from_channel_value(val: &Value) -> Result<Self, Exception> {
        let ch = val.try_to_rust_type::<Channel>()?;
        Ok(Self {
            channel: (*ch).clone(),
        })
    }

    pub async fn recv(&self) -> Result<Value, Exception> {
        let event = crate::channels::recv_event(self.channel.clone());
        crate::event::perform_base(&event).await
    }
}
