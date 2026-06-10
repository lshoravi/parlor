use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Once};
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
fn test_parlor_basic() {
    run_scheme_test("parlor_basic.scm");
}

#[test]
fn test_parlor_channels() {
    run_scheme_test("parlor_channels.scm");
}

#[test]
fn test_parlor_conditions() {
    run_scheme_test("parlor_conditions.scm");
}

#[test]
fn test_parlor_compose() {
    run_scheme_test("parlor_compose.scm");
}

#[test]
fn test_parlor_tasks() {
    run_scheme_test("parlor_tasks.scm");
}

#[test]
fn test_parlor_integration() {
    run_scheme_test("parlor_integration.scm");
}

#[test]
fn test_parlor_stress_channels() {
    run_scheme_test("parlor_stress_channels.scm");
}

#[test]
fn test_parlor_stress_choose() {
    run_scheme_test("parlor_stress_choose.scm");
}

#[test]
fn test_parlor_stress_tasks() {
    run_scheme_test("parlor_stress_tasks.scm");
}

#[test]
fn test_parlor_stress_conditions() {
    run_scheme_test("parlor_stress_conditions.scm");
}

#[test]
fn test_parlor_stress_mixed() {
    run_scheme_test("parlor_stress_mixed.scm");
}

#[test]
fn test_parlor_api_coverage() {
    run_scheme_test("parlor_api_coverage.scm");
}

#[test]
fn test_parlor_gc_double_decrement() {
    run_scheme_test("parlor_gc_double_decrement.scm");
}

#[test]
fn test_parlor_gc_then_spawn() {
    run_scheme_test("parlor_gc_then_spawn.scm");
}

#[test]
fn test_parlor_io() {
    run_scheme_test("parlor_io.scm");
}

#[test]
fn test_parlor_echo_server() {
    run_scheme_test("parlor_echo_server.scm");
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

// --- Gap #1: guard-evt + choose where all try-paths fail (block path) ---

#[test]
fn test_parlor_guard_block_path() {
    run_scheme_test("parlor_guard_block_path.scm");
}

// --- Gap #2: Channel protocol contention tests ---

#[tokio::test]
async fn test_rendezvous_contention_no_lost_messages() {
    let ch = Channel::new_rendezvous();
    let val = Value::from_rust_type(ch);

    let total_messages = 100usize;
    let received = Arc::new(std::sync::Mutex::new(Vec::new()));

    let mut recv_handles = Vec::new();
    for _ in 0..total_messages {
        let consumer = Consumer::from_channel_value(&val).unwrap();
        let recv_log = received.clone();
        recv_handles.push(tokio::spawn(async move {
            let v = tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await;
            if let Ok(Ok(v)) = v {
                recv_log.lock().unwrap().push(v);
            }
        }));
    }

    let mut send_handles = Vec::new();
    for i in 0..total_messages {
        let producer = Producer::from_channel_value(&val).unwrap();
        send_handles.push(tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_millis(500),
                producer.send(Value::from(i as i64)),
            )
            .await
            .expect("send timed out")
            .expect("send failed");
        }));
    }

    for h in send_handles {
        h.await.unwrap();
    }
    for h in recv_handles {
        h.await.unwrap();
    }

    let got = received.lock().unwrap();
    assert_eq!(got.len(), total_messages, "lost messages under contention");
    let expected: Vec<Value> = (0..total_messages as i64).map(Value::from).collect();
    let mut sorted = got.clone();
    sorted.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    let mut expected_sorted = expected;
    expected_sorted.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    assert_eq!(sorted, expected_sorted, "messages lost or duplicated under contention");
}

#[tokio::test]
async fn test_rendezvous_many_senders_one_receiver() {
    let ch = Channel::new_rendezvous();
    let val = Value::from_rust_type(ch);
    let consumer = Consumer::from_channel_value(&val).unwrap();

    let num_senders = 50;
    let mut send_handles = Vec::new();
    for i in 0..num_senders {
        let producer = Producer::from_channel_value(&val).unwrap();
        send_handles.push(tokio::spawn(async move {
            producer.send(Value::from(i as i64)).await.unwrap();
        }));
    }

    let mut received = Vec::new();
    for _ in 0..num_senders {
        let v = tokio::time::timeout(Duration::from_millis(500), consumer.recv())
            .await
            .expect("recv timed out")
            .unwrap();
        received.push(v);
    }

    for h in send_handles {
        h.await.unwrap();
    }

    assert_eq!(received.len(), num_senders);
    let expected: Vec<Value> = (0..num_senders as i64).map(Value::from).collect();
    let mut sorted = received.clone();
    sorted.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    let mut expected_sorted = expected;
    expected_sorted.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    assert_eq!(sorted, expected_sorted, "messages lost or duplicated");
}

#[tokio::test]
async fn test_gc_after_many_cancelled_operations() {
    let ch = Channel::new_rendezvous();
    let val = Value::from_rust_type(ch);

    for _ in 0..80 {
        let ghost = Consumer::from_channel_value(&val).unwrap();
        let _ = tokio::time::timeout(Duration::from_millis(1), ghost.recv()).await;
    }

    let producer = Producer::from_channel_value(&val).unwrap();
    let consumer = Consumer::from_channel_value(&val).unwrap();

    let send_handle = tokio::spawn(async move {
        producer.send(Value::from(999i64)).await.unwrap();
    });

    let result = tokio::time::timeout(Duration::from_millis(500), consumer.recv())
        .await
        .expect("recv timed out after 80 ghost waiters")
        .unwrap();
    assert_eq!(result, Value::from(999i64));
    send_handle.await.unwrap();
}

// --- Gap #3: timer-operation with absolute timestamps ---

#[test]
fn test_parlor_timer_operation() {
    run_scheme_test("parlor_timer_operation.scm");
}

// --- Gap #4: Buffered channel at-capacity blocking ---

#[tokio::test]
async fn test_buffered_at_capacity_blocks_then_drains() {
    let ch = Channel::new_buffered(2);
    let val = Value::from_rust_type(ch);
    let producer = Producer::from_channel_value(&val).unwrap();
    let consumer = Consumer::from_channel_value(&val).unwrap();

    producer.send(Value::from(1i64)).await.unwrap();
    producer.send(Value::from(2i64)).await.unwrap();

    let send_complete = Arc::new(AtomicUsize::new(0));
    let sc = send_complete.clone();
    let p2 = Producer::from_channel_value(&val).unwrap();
    let send_handle = tokio::spawn(async move {
        p2.send(Value::from(3i64)).await.unwrap();
        sc.store(1, Ordering::Release);
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        send_complete.load(Ordering::Acquire),
        0,
        "third send should be blocked while buffer is full"
    );

    // Drain all three values. The blocked sender is in putq; a recv will
    // first pop from the buffer, and eventually find the sender in putq.
    let v1 = tokio::time::timeout(Duration::from_millis(200), consumer.recv())
        .await.expect("recv 1 timed out").unwrap();
    let v2 = tokio::time::timeout(Duration::from_millis(200), consumer.recv())
        .await.expect("recv 2 timed out").unwrap();
    let v3 = tokio::time::timeout(Duration::from_millis(200), consumer.recv())
        .await.expect("recv 3 timed out").unwrap();

    assert_eq!(v1, Value::from(1i64));
    assert_eq!(v2, Value::from(2i64));
    assert_eq!(v3, Value::from(3i64));

    send_handle.await.unwrap();
}

#[tokio::test]
async fn test_buffered_try_send_at_capacity() {
    let ch = Channel::new_buffered(2);
    let val = Value::from_rust_type(ch);
    let producer = Producer::from_channel_value(&val).unwrap();
    let consumer = Consumer::from_channel_value(&val).unwrap();

    producer.try_send(Value::from(1i64)).unwrap();
    producer.try_send(Value::from(2i64)).unwrap();
    assert!(producer.try_send(Value::from(3i64)).is_err(), "try_send should fail at capacity");

    let _ = consumer.recv().await.unwrap();
    producer.try_send(Value::from(3i64)).unwrap();
}

// --- Gap #5: readable-evt / writable-evt readiness detection ---
// Note: The Scheme-level readable-evt/writable-evt bridges have a bug where
// port.raw_fd() uses blocking_lock() inside an async context. These tests
// exercise the underlying readiness mechanism at the Rust level instead.

#[tokio::test]
async fn test_tcp_readable_writable_readiness() {
    use std::sync::Arc;
    use parlor::event::{BaseEvent, BlockFn, CancelFn, DoFn, Flag, OpState, PollFn, ResumeTx, cas, perform_base};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let client = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();

    // writable-evt on client: TCP send buffer is empty, should be immediately writable.
    {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = client.as_raw_fd();
            let cancel_fn: CancelFn = Arc::new(|| {});
            let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
                let handle = tokio::spawn(async move {
                    let owned = unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) }
                        .try_clone_to_owned().unwrap();
                    let async_fd = tokio::io::unix::AsyncFd::new(owned).unwrap();
                    let _ = async_fd.ready(tokio::io::Interest::WRITABLE).await;
                    if cas(&flag, OpState::Waiting, OpState::Synched) {
                        let _ = tx.send(Value::from(true));
                    }
                });
                Some(handle.abort_handle())
            });
            let poll_fn: PollFn = Arc::new(|| false);
            let do_fn: DoFn = Arc::new(|| None);
            let evt = BaseEvent { poll_fn, do_fn, block_fn, cancel_fn, wrap_fns: Vec::new() };
            let result = tokio::time::timeout(Duration::from_millis(500), perform_base(&evt))
                .await.expect("writable-evt timed out").unwrap();
            assert_eq!(result, Value::from(true));
        }
    }

    // readable-evt on server: no data sent yet, should timeout.
    {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = server.as_raw_fd();
            let cancel_fn: CancelFn = Arc::new(|| {});
            let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
                let handle = tokio::spawn(async move {
                    let owned = unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) }
                        .try_clone_to_owned().unwrap();
                    let async_fd = tokio::io::unix::AsyncFd::new(owned).unwrap();
                    let _ = async_fd.ready(tokio::io::Interest::READABLE).await;
                    if cas(&flag, OpState::Waiting, OpState::Synched) {
                        let _ = tx.send(Value::from(true));
                    }
                });
                Some(handle.abort_handle())
            });
            let poll_fn: PollFn = Arc::new(|| false);
            let do_fn: DoFn = Arc::new(|| None);
            let evt = BaseEvent { poll_fn, do_fn, block_fn, cancel_fn, wrap_fns: Vec::new() };
            let result = tokio::time::timeout(Duration::from_millis(100), perform_base(&evt)).await;
            assert!(result.is_err(), "readable-evt should timeout with no data");
        }
    }

    // Send data from client, then readable-evt on server should fire.
    {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            use tokio::io::AsyncWriteExt;
            let mut client = client;
            client.write_all(b"hello").await.unwrap();
            client.flush().await.unwrap();

            let fd = server.as_raw_fd();
            let cancel_fn: CancelFn = Arc::new(|| {});
            let block_fn: BlockFn = Arc::new(move |flag: Flag, tx: ResumeTx| {
                let handle = tokio::spawn(async move {
                    let owned = unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) }
                        .try_clone_to_owned().unwrap();
                    let async_fd = tokio::io::unix::AsyncFd::new(owned).unwrap();
                    let _ = async_fd.ready(tokio::io::Interest::READABLE).await;
                    if cas(&flag, OpState::Waiting, OpState::Synched) {
                        let _ = tx.send(Value::from(true));
                    }
                });
                Some(handle.abort_handle())
            });
            let poll_fn: PollFn = Arc::new(|| false);
            let do_fn: DoFn = Arc::new(|| None);
            let evt = BaseEvent { poll_fn, do_fn, block_fn, cancel_fn, wrap_fns: Vec::new() };
            let result = tokio::time::timeout(Duration::from_millis(500), perform_base(&evt))
                .await.expect("readable-evt timed out after data sent").unwrap();
            assert_eq!(result, Value::from(true));
        }
    }
}

// --- Gap #6: Negative/error-path tests ---

#[test]
fn test_parlor_error_paths() {
    run_scheme_test("parlor_error_paths.scm");
}

#[tokio::test]
async fn test_rust_buffered_channel_capacity_zero() {
    let ch = Channel::new_buffered(0);
    let val = Value::from_rust_type(ch);
    let producer = Producer::from_channel_value(&val).unwrap();
    let result = producer.try_send(Value::from(1i64));
    assert!(result.is_err(), "capacity-0 buffered channel should reject sends");
}

// --- Gap #7: Rendezvous send timeout via choose ---

#[test]
fn test_parlor_send_timeout() {
    run_scheme_test("parlor_send_timeout.scm");
}

#[test]
fn test_parlor_always_never() {
    run_scheme_test("parlor_always_never.scm");
}

#[test]
fn test_parlor_poll_do_split() {
    run_scheme_test("parlor_poll_do_split.scm");
}

#[test]
fn test_parlor_with_nack() {
    run_scheme_test("parlor_with_nack.scm");
}

#[test]
fn test_parlor_spawn_join() {
    run_scheme_test("parlor_spawn_join.scm");
}
