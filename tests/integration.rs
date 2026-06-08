use scheme_rs::runtime::Runtime;
use scheme_rs::value::Value;
use scheme_rs_cml::channels::CmlChannel;
use scheme_rs_cml::producer::{CmlConsumer, CmlProducer};
use scheme_rs_cml as _;
use std::path::PathBuf;

fn run_scheme_test(filename: &str) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let scheme_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scheme");
        unsafe { std::env::set_var("SCHEME_RS_LOAD_PATH", &scheme_dir) };

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

#[tokio::test]
async fn test_producer_consumer_roundtrip() {
    let ch = CmlChannel::new_buffered(10);
    let val = Value::from_rust_type(ch);
    let producer = CmlProducer::from_channel_value(&val).unwrap();
    let consumer = CmlConsumer::from_channel_value(&val).unwrap();

    producer.send(Value::from(42i64)).await.unwrap();
    let result = consumer.recv().await.unwrap();
    assert_eq!(result, Value::from(42i64));
}

#[tokio::test]
async fn test_producer_try_send_full() {
    let ch = CmlChannel::new_buffered(1);
    let val = Value::from_rust_type(ch);
    let producer = CmlProducer::from_channel_value(&val).unwrap();

    producer.try_send(Value::from(1i64)).unwrap();
    let err = producer.try_send(Value::from(2i64));
    assert!(err.is_err());
}

#[tokio::test]
async fn test_producer_consumer_multiple() {
    let ch = CmlChannel::new_buffered(10);
    let val = Value::from_rust_type(ch);
    let producer = CmlProducer::from_channel_value(&val).unwrap();
    let consumer = CmlConsumer::from_channel_value(&val).unwrap();

    for i in 0..10i64 {
        producer.send(Value::from(i)).await.unwrap();
    }
    for i in 0..10i64 {
        let result = consumer.recv().await.unwrap();
        assert_eq!(result, Value::from(i));
    }
}
