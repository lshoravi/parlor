use std::path::PathBuf;
use std::sync::Once;
use std::time::Duration;

use scheme_rs::runtime::Runtime;
use scheme_rs::value::Value;
use parlor::channels::Channel;
use parlor::producer::{Consumer, Producer};
use parlor as _;

static INIT_ENV: Once = Once::new();

fn run_scheme_test(filename: &str) {
    INIT_ENV.call_once(|| {
        let scheme_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scheme");
        unsafe { std::env::set_var("SCHEME_RS_LOAD_PATH", &scheme_dir) };
    });

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let runtime = Runtime::new();
        let test_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join(filename);
        runtime
            .run_program(&test_path)
            .await
            .expect(&format!("scheme test {filename} failed"));
    });
}

#[test]
fn test_cml_basic() {
    run_scheme_test("cml_basic.scm");
}

#[test]
fn test_cml_channels() {
    run_scheme_test("cml_channels.scm");
}

#[test]
fn test_cml_conditions() {
    run_scheme_test("cml_conditions.scm");
}

#[test]
fn test_cml_compose() {
    run_scheme_test("cml_compose.scm");
}

#[test]
fn test_cml_tasks() {
    run_scheme_test("cml_tasks.scm");
}

#[test]
fn test_cml_integration() {
    run_scheme_test("cml_integration.scm");
}

#[test]
fn test_cml_stress_channels() {
    run_scheme_test("cml_stress_channels.scm");
}

#[test]
fn test_cml_stress_choose() {
    run_scheme_test("cml_stress_choose.scm");
}

#[test]
fn test_cml_stress_tasks() {
    run_scheme_test("cml_stress_tasks.scm");
}

#[test]
fn test_cml_stress_conditions() {
    run_scheme_test("cml_stress_conditions.scm");
}

#[test]
fn test_cml_stress_mixed() {
    run_scheme_test("cml_stress_mixed.scm");
}

#[test]
fn test_cml_api_coverage() {
    run_scheme_test("cml_api_coverage.scm");
}

#[test]
fn test_cml_gc_double_decrement() {
    run_scheme_test("cml_gc_double_decrement.scm");
}

#[test]
fn test_cml_gc_then_spawn() {
    run_scheme_test("cml_gc_then_spawn.scm");
}

#[test]
fn test_cml_io() {
    run_scheme_test("cml_io.scm");
}

#[test]
fn test_cml_echo_server() {
    run_scheme_test("cml_echo_server.scm");
}

#[tokio::test]
async fn test_producer_consumer_roundtrip() {
    let ch = Channel::new_buffered(10);
    let val = Value::from_rust_type(ch);
    let producer = Producer::from_channel_value(&val).unwrap();
    let consumer = Consumer::from_channel_value(&val).unwrap();

    producer.send(Value::from(42i64)).await.unwrap();
    let result = consumer.recv().await.unwrap();
    assert_eq!(result, Value::from(42i64));
}

#[tokio::test]
async fn test_producer_try_send_full() {
    let ch = Channel::new_buffered(1);
    let val = Value::from_rust_type(ch);
    let producer = Producer::from_channel_value(&val).unwrap();

    producer.try_send(Value::from(1i64)).unwrap();
    let err = producer.try_send(Value::from(2i64));
    assert!(err.is_err());
}

#[tokio::test]
async fn test_producer_consumer_multiple() {
    let ch = Channel::new_buffered(10);
    let val = Value::from_rust_type(ch);
    let producer = Producer::from_channel_value(&val).unwrap();
    let consumer = Consumer::from_channel_value(&val).unwrap();

    for i in 0..10i64 {
        producer.send(Value::from(i)).await.unwrap();
    }
    for i in 0..10i64 {
        let result = consumer.recv().await.unwrap();
        assert_eq!(result, Value::from(i));
    }
}

#[tokio::test]
async fn test_cancelled_recv_does_not_steal_message() {
    let ch = Channel::new_rendezvous();
    let val = Value::from_rust_type(ch);
    let consumer = Consumer::from_channel_value(&val).unwrap();
    let ghost = Consumer::from_channel_value(&val).unwrap();

    // Block a recv, then cancel it via timeout. This leaves a ghost
    // RecvWaiter in getq with flag=Waiting.
    let _ = tokio::time::timeout(Duration::from_millis(10), ghost.recv()).await;

    // Send a message. The try_fn will find the ghost waiter, CAS its flag
    // Waiting→Synched, and try to deliver through the ghost's dead tx.
    // The send thinks it succeeded, but the message goes nowhere.
    let send_handle = tokio::spawn({
        let val = val.clone();
        async move {
            let p = Producer::from_channel_value(&val).unwrap();
            p.send(Value::from(42i64)).await.unwrap();
        }
    });

    // A real receiver should get the message. Without a Drop guard on the
    // flag, the ghost stole it and this times out.
    let result = tokio::time::timeout(Duration::from_millis(200), consumer.recv()).await;
    assert!(result.is_ok(), "message was lost to a ghost waiter from a cancelled recv");
    assert_eq!(result.unwrap().unwrap(), Value::from(42i64));

    send_handle.await.unwrap();
}

#[tokio::test]
async fn test_cancelled_send_message_not_delivered() {
    let ch = Channel::new_rendezvous();
    let val = Value::from_rust_type(ch);
    let consumer = Consumer::from_channel_value(&val).unwrap();

    // Block a send, then cancel it. The ghost SendWaiter stays in putq
    // with flag=Waiting and message=99.
    let ghost_producer = Producer::from_channel_value(&val).unwrap();
    let _ = tokio::time::timeout(
        Duration::from_millis(10),
        ghost_producer.send(Value::from(99i64)),
    ).await;

    // Now do a real send + recv. The receiver should get 42, not the
    // ghost's 99.
    let send_handle = tokio::spawn({
        let val = val.clone();
        async move {
            let p = Producer::from_channel_value(&val).unwrap();
            p.send(Value::from(42i64)).await.unwrap();
        }
    });

    let result = tokio::time::timeout(Duration::from_millis(200), consumer.recv()).await;
    assert!(result.is_ok(), "recv timed out");
    let received = result.unwrap().unwrap();
    assert_eq!(received, Value::from(42i64), "receiver got ghost message instead of real one");

    send_handle.await.unwrap();
}
