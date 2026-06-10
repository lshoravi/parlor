# Parlor

Parallel Concurrent ML (PCML) is a programming model for parallel, concurrent programming where synchronous events are first-class values that can be constructed, composed, and synchronized on later. The idea is that cross-thread (in this case green threads) communication doesn't happen via shared memory but via "events". Parlor is a PCML(-ish) implementation in Rust on top of tokio, built for scheme-rs.

## What it is

Event combinators for concurrent Scheme programs. Events are values: you build them with constructors like `recv-evt` or `sleep-evt`, compose them with `choose` and `wrap`, and synchronize with `sync`. Nothing happens until you call `sync`.

The core combinators (`choose`, `wrap`, `guard-evt`, `with-nack`) follow Reppy's CML design. Channels are synchronous by default (rendezvous). Conditions, timers, I/O readiness, and task joins are all expressed as events and compose uniformly.

Requires scheme-rs with async and tokio features enabled. This has a benefit: you can run this code anywhere, no special executor/"run-tasks" call anywhere.

## What it isn't

This is not SML/NJ CML. There is no preemptive scheduling; everything runs cooperatively on the tokio executor. It does not implement composable asynchronous events (Ziarek et al., PLDI 2011). Buffered channels are a pragmatic extension that breaks the rendezvous property.

## Shortcomings

`poll_fn` closures must be side-effect-free since they may be called multiple times per sync. `guard-evt` thunks run on the tokio executor, not inline. Events are not polymorphic over their result type at the Scheme level. No preemption, as tokio doesn't have preemption.

## Examples

```scheme
;; rendezvous send/recv
(import (parlor) (parlor channels) (parlor spawn))

(define ch (make-channel))
(spawn-task (lambda () (send ch 'hello)))
(assert (eq? (recv ch) 'hello))
```

```scheme
;; select with timeout
(define result
  (select (wrap (recv-evt ch) (lambda (v) (list 'got v)))
          (wrap (sleep-evt 1.0) (lambda (_) 'timeout))))
```

```scheme
;; with-nack: cancel a watcher when the branch loses
(define done (make-condition))

(select
  (with-nack
    (lambda (nack)
      (spawn-task
        (lambda ()
          (select (wrap nack (lambda (_) 'lost))
                  (wrap (wait-evt done) (lambda (_) 'won)))))
      (wrap (sleep-evt 10.0) (lambda (_) 'slow))))
  (always-evt 'fast))
```

```scheme
;; join-evt: await a spawned task as an event
(define f (spawn-task (lambda () (* 6 7))))
(assert (= (sync (join-evt f)) 42))
```

## Modules

| Library | Provides |
|---|---|
| `(parlor)` | `sync` `select` `choose` `wrap` `guard-evt` `with-nack` `always-evt` `never-evt` `make-custom-event` |
| `(parlor channels)` | `make-channel` `send-evt` `recv-evt` `send` `recv` |
| `(parlor conditions)` | `make-condition` `signal!` `wait-evt` `wait` `make-notifier` `notify!` `notify-evt` |
| `(parlor timers)` | `sleep-evt` `sleep` `timer-operation` |
| `(parlor io)` | `accept-evt` `readable-evt` `writable-evt` `accept` `connect-tcp` `bind-tcp` `listener-address` |
| `(parlor spawn)` | `spawn-task` `join-evt` |

See [docs/guide.md](docs/guide.md) for the programming model and [docs/api.md](docs/api.md) for the full API reference.
