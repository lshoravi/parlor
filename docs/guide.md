# Parlor Programming Guide

## Events are values

An event describes something that *could* happen: a message arriving, a timer expiring, a connection being accepted. Events don't do anything when created. They are inert values you can store, pass to functions, and compose. Only `sync` forces the event to resolve.

The same event can be synced multiple times.

```scheme
(define ch (make-channel 5))
(define se (send-evt ch 'ping))
(define re (recv-evt ch))

(sync se)
(sync se)
(sync se)
(assert (eq? (sync re) 'ping))
(assert (eq? (sync re) 'ping))
(assert (eq? (sync re) 'ping))
```

`always-evt` and `never-evt` are the identity elements. `(always-evt v)` fires immediately with value `v`. `(never-evt)` never fires. In a `choose`, `never-evt` alternatives are inert and `always-evt` alternatives are always available.

```scheme
(sync (choose (never-evt) (always-evt 42)))  ; => 42
```

## The sync protocol

Each base event has three internal phases: poll, do, block. When you call `sync`:

1. **Poll** checks if the event can fire without waiting (e.g., a message is in the queue). This must be side-effect-free.
2. **Do** attempts to atomically claim the result. It may fail under contention.
3. **Block** registers a waiter and suspends the current task until the event fires.

You never call these directly. They exist so that `choose` can fairly select among multiple ready alternatives: it polls all branches, shuffles the enabled set, then tries `do` in random order. If none succeed, all branches block concurrently and the first to fire wins.

This split is why `choose` is fair. Without it, the first alternative would always be preferred.

## Composition

**choose** selects among alternatives. When multiple are ready, the winner is chosen at random. Nested `choose` calls flatten.

```scheme
(sync (choose
        (wrap (recv-evt ch1) (lambda (v) (cons 'ch1 v)))
        (wrap (recv-evt ch2) (lambda (v) (cons 'ch2 v)))
        (wrap (sleep-evt 5.0) (lambda (_) 'timeout))))
```

**wrap** transforms the result of an event. Wraps compose left-to-right (innermost applied first). Wrapping distributes over `choose` and `with-nack`.

```scheme
(sync (wrap (wrap (sleep-evt 0.0)
                  (lambda (_) 10))
            (lambda (x) (* x 2))))  ; => 20
```

**guard-evt** defers event construction to sync time. The thunk runs asynchronously on the tokio executor and must return an event. Use it when the event itself needs fresh state each time it's synced.

```scheme
;; fresh accept-evt each iteration
(sync (guard-evt (lambda () (accept-evt listener))))
```

**with-nack** provides cancellation feedback. The thunk receives a nack event that fires if this branch loses the `choose`. It must return an event. `with-nack` only makes sense inside `choose`.

```scheme
(sync (choose
        (with-nack
          (lambda (nack)
            ;; launch cleanup watcher
            (tokio/spawn (lambda () (sync nack) (do-cleanup)))
            (recv-evt request-ch)))
        (wrap (sleep-evt 5.0) (lambda (_) 'timeout))))
```

## Patterns

**Timeout.** The most common pattern: race a real event against `sleep-evt`.

```scheme
(define (recv-with-timeout ch seconds)
  (sync (choose
          (recv-evt ch)
          (wrap (sleep-evt seconds) (lambda (_) 'timeout)))))
```

**RPC with cleanup.** Use `with-nack` to launch a watcher that cleans up if the branch loses. The watcher must also know when to stop if the branch *wins*, otherwise it leaks. Use a done condition: the watcher chooses between the nack and the done signal.

```scheme
(define (rpc-with-timeout request-ch timeout-secs)
  (let ((done (make-condition)))
    (sync (choose
            (with-nack
              (lambda (nack)
                (tokio/spawn
                  (lambda ()
                    (let ((reason (sync (choose
                                    (wrap nack (lambda (_) 'lost))
                                    (wrap (wait-evt done) (lambda (_) 'won))))))
                      (when (eq? reason 'lost)
                        (cancel-pending-rpc)))))
                (wrap (recv-evt request-ch)
                      (lambda (v) (signal! done) v))))
            (wrap (sleep-evt timeout-secs)
                  (lambda (_) (signal! done) 'timeout))))))
```

Without the done condition, the watcher task sits forever waiting on a nack that will never fire (because its branch won).

**Fan-in.** Multiple producers, one consumer, buffered channel.

```scheme
(define ch (make-channel 64))

;; N producers
(do ((i 0 (+ i 1))) ((= i 10))
  (tokio/spawn (lambda () (send ch (compute i)))))

;; consumer
(let loop ((results '()))
  (if (= (length results) 10)
      results
      (loop (cons (recv ch) results))))
```

**Worker pool with join-evt.** Spawn tasks, collect results as events.

```scheme
(define futures
  (map (lambda (job)
         (tokio/spawn (lambda () (process job))))
       jobs))

(define results
  (map (lambda (f) (sync (join-evt f))) futures))
```

## Gotchas

**poll_fn must be pure.** The poll phase can run multiple times per sync (once per `choose` alternative scan). Side effects in poll will cause bugs.

**guard-evt is async.** The thunk runs on the tokio executor, not inline. It runs during the block phase, so it adds latency compared to a pre-built event.

**with-nack watchers need a done signal.** If the branch wins, the nack never fires. A watcher that only listens for the nack will leak. Choose between the nack and a done condition (see the RPC pattern above).

**Buffered channels don't rendezvous.** A `send-evt` on a buffered channel succeeds immediately if there is buffer space, without a matching receiver. This breaks the CML guarantee that send and recv synchronize together. Use `(make-channel)` (no argument) for true rendezvous.

**send-evt returns void.** The value produced by `send-evt` is `#f`, not the sent message. `recv-evt` produces the received message.

**never-evt blocks forever.** `(sync (never-evt))` will hang. It is only useful as an identity inside `choose`.
