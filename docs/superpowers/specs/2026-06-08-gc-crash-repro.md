# Reproducing the GC crash

## Crash signature

Signal: SIGSEGV (invalid memory reference) or SIGTRAP (trace/breakpoint trap)

```
thread #N, stop reason = EXC_BAD_ACCESS (code=2, address=0x...)
frame #0: HeapObject::visit_children at collection.rs:248
    unsafe { (*self.header.as_ref().get()).vtable.visit_children }
```

The GC collector thread reads a vtable pointer from a heap object's header and the pointer is invalid.

## Prerequisites

```
git clone ssh://git@github.com/lshoravi/scheme-rs-cml.git
cd scheme-rs-cml
git checkout 731dd17   # or latest master
```

## Reproduction method 1: cargo test (highest repro rate)

Run the full test suite sequentially:

```bash
cargo test --test integration -- --test-threads=1
```

Expected: crashes with SIGSEGV or SIGTRAP. Repro rate: ~100% (crashes somewhere during the 15-test sequence, usually around test 10-12).

Each individual test passes:

```bash
cargo test --test integration test_cml_basic -- --test-threads=1       # pass
cargo test --test integration test_cml_stress_choose -- --test-threads=1  # pass
```

The crash requires accumulated state from running multiple tests in sequence within one process.

## Reproduction method 2: stress_choose in a loop (moderate repro rate)

```bash
for i in $(seq 1 30); do
  result=$(cargo test --test integration test_cml_stress_choose -- --test-threads=1 2>&1 | tail -3)
  if echo "$result" | grep -q 'signal:'; then
    echo "CRASH on run $i"
  fi
done
```

Expected: ~10-30% of runs crash. The test does 100 iterations of `choose(recv-evt, wrap(sleep-evt, lambda))` — each iteration creates and discards event objects.

## Reproduction method 3: lldb backtrace

```bash
cargo test --no-run 2>&1 | grep -o 'target/[^ ]*integration[^ )]*'
# gives something like: target/debug/deps/integration-XXXX

for i in $(seq 1 10); do
  result=$(lldb -b -o 'run -- --test-threads=1' -o 'bt' \
    -- target/debug/deps/integration-XXXX 2>&1)
  if echo "$result" | grep -q 'EXC_BAD_ACCESS\|SIGSEGV\|SIGTRAP'; then
    echo "$result" | grep -A 20 'stop reason'
    break
  fi
done
```

## What the crashing test does

`tests/cml_stress_choose.scm` — the most reliable single-test trigger:

```scheme
(let ((ch (make-channel 1)))
  (do ((iter 0 (+ iter 1)))
      ((= iter 100))
    (send ch iter)
    (sync (choose (recv-evt ch)
                  (wrap (sleep-evt 10.0) (lambda (_) 'timeout))))))
```

Each iteration:
1. `send ch iter` — pushes value into buffered channel (try-path, synchronous)
2. `choose(recv-evt ch, wrap(sleep-evt 10.0, lambda))` — creates a ChoiceEvent with two BaseEvents
3. `sync` calls `perform_choice` — try-path finds the recv-evt ready (buffer non-empty), returns immediately
4. The ChoiceEvent and both BaseEvents become garbage
5. The timer BaseEvent contains Arc'd closures (block_fn captures a Duration, wrap lambda is a Gc'd Procedure)
6. The Gc-managed objects are eventually collected by the GC collector thread

The crash happens when the collector visits objects from a previous iteration while the main thread is allocating objects for the current iteration.

## Factors that affect repro rate

- **Number of preceding tests**: more tests = more accumulated Gc objects = higher GC pressure = higher repro rate. Running all 15 tests sequentially is ~100%.
- **Iteration count in stress_choose**: 100 iterations at ~10-30%, original 1000-5000 iterations were higher. Reduced for timeout reasons.
- **Binary layout**: adding/removing unrelated Rust code shifts the race window. A clean rebuild (`cargo clean && cargo test`) may change the repro rate.
- **Platform**: tested on macOS Apple Silicon (ARM64). Not tested on x86_64 or Linux.

## What does NOT crash

- Pure scheme-rs tests (no CML): `cargo test` in the scheme-rs repo passes reliably
- CML tests without `choose`: `test_cml_basic`, `test_cml_channels`, `test_cml_conditions` pass individually
- CML `choose` without timers: `choose(recv-evt, recv-evt)` does not crash
- CML `sync(sleep-evt)` without `choose`: single-event sync does not crash
- Running tests in parallel (default `cargo test` without `--test-threads=1`): crashes less often because each test gets its own tokio runtime with less accumulated state, but still crashes ~20-30% of full-suite runs
