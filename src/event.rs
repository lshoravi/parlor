use std::sync::Arc;

use scheme_rs::gc::Trace;
use scheme_rs::proc::Procedure;
use scheme_rs::records::{RecordTypeDescriptor, SchemeCompatible, rtd};

/// Duration stored as nanoseconds (u64 implements Trace).
#[derive(Clone, Debug, Trace)]
pub enum Event {
    Timer { nanos: u64 },
    Custom { thunk: Procedure },
    Wrapped { inner: Box<Event>, transform: Procedure },
    Choice { alternatives: Vec<Event> },
    Guard { thunk: Procedure },
}

impl SchemeCompatible for Event {
    fn rtd() -> Arc<RecordTypeDescriptor> {
        rtd!(
            name: "cml-event",
            opaque: true,
            sealed: true,
        )
    }
}
