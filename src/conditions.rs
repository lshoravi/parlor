use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use scheme_rs::exceptions::Exception;
use scheme_rs::gc::{OpaqueGcPtr, Trace};
use scheme_rs::records::{rtd, RecordTypeDescriptor, SchemeCompatible};
use scheme_rs::registry::bridge;
use scheme_rs::value::Value;
use tokio::sync::{Notify, Semaphore};

use crate::event::Event;

#[derive(Debug, Clone)]
pub struct Condition {
    pub notify: Arc<Notify>,
    pub signalled: Arc<AtomicBool>,
}

unsafe impl Trace for Condition {
    unsafe fn visit_children(&self, _visitor: &mut dyn FnMut(OpaqueGcPtr)) {}
    unsafe fn finalize(&mut self) {
        unsafe { std::ptr::drop_in_place(self as *mut Self) }
    }
}

impl SchemeCompatible for Condition {
    fn rtd() -> Arc<RecordTypeDescriptor> {
        rtd!(name: "cml-condition", opaque: true, sealed: true)
    }
}

#[derive(Debug, Clone)]
pub struct CmlNotifier {
    pub sem: Arc<Semaphore>,
}

unsafe impl Trace for CmlNotifier {
    unsafe fn visit_children(&self, _visitor: &mut dyn FnMut(OpaqueGcPtr)) {}
    unsafe fn finalize(&mut self) {
        unsafe { std::ptr::drop_in_place(self as *mut Self) }
    }
}

impl SchemeCompatible for CmlNotifier {
    fn rtd() -> Arc<RecordTypeDescriptor> {
        rtd!(name: "cml-notifier", opaque: true, sealed: true)
    }
}

#[bridge(name = "%make-condition", lib = "(cml conditions bridge)")]
pub async fn make_condition() -> Result<Vec<Value>, Exception> {
    let cond = Condition {
        notify: Arc::new(Notify::new()),
        signalled: Arc::new(AtomicBool::new(false)),
    };
    Ok(vec![Value::from_rust_type(cond)])
}

#[bridge(name = "%signal!", lib = "(cml conditions bridge)")]
pub async fn signal(cv_val: &Value) -> Result<Vec<Value>, Exception> {
    let cv = cv_val.try_to_rust_type::<Condition>()?;
    let was_first = !cv.signalled.swap(true, Ordering::SeqCst);
    if was_first {
        cv.notify.notify_waiters();
    }
    Ok(vec![Value::from(was_first)])
}

#[bridge(name = "%wait-evt", lib = "(cml conditions bridge)")]
pub async fn wait_evt(cv_val: &Value) -> Result<Vec<Value>, Exception> {
    let cv = cv_val.try_to_rust_type::<Condition>()?;
    let event = Event::ConditionWait {
        notify: cv.notify.clone(),
        signalled: cv.signalled.clone(),
    };
    Ok(vec![Value::from_rust_type(event)])
}

#[bridge(name = "%make-notifier", lib = "(cml conditions bridge)")]
pub async fn make_notifier() -> Result<Vec<Value>, Exception> {
    let n = CmlNotifier {
        sem: Arc::new(Semaphore::new(0)),
    };
    Ok(vec![Value::from_rust_type(n)])
}

#[bridge(name = "%notify!", lib = "(cml conditions bridge)")]
pub async fn notify(n_val: &Value) -> Result<Vec<Value>, Exception> {
    let n = n_val.try_to_rust_type::<CmlNotifier>()?;
    n.sem.add_permits(1);
    Ok(vec![])
}

#[bridge(name = "%notify-evt", lib = "(cml conditions bridge)")]
pub async fn notify_evt(n_val: &Value) -> Result<Vec<Value>, Exception> {
    let n = n_val.try_to_rust_type::<CmlNotifier>()?;
    let event = Event::NotifierWait {
        sem: n.sem.clone(),
    };
    Ok(vec![Value::from_rust_type(event)])
}
