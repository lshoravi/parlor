# Parlor API Reference

## Core — `(parlor)`

### sync
`(sync evt) -> value`

Synchronize on an event. Blocks until the event fires, returns its value.

### choose
`(choose evt ...) -> event`

Combine events into a choice. When synced, at most one alternative fires. Among ready alternatives, the winner is chosen at random. Nested choices flatten.

### wrap
`(wrap evt proc) -> event`

Transform the result of an event. `proc` is called with the event's value when it fires. Wraps compose left-to-right (innermost first). Distributes over `choose` and `with-nack`.

### guard-evt
`(guard-evt thunk) -> event`

Defer event construction to sync time. `thunk` is a zero-argument procedure that runs asynchronously and must return an event. The returned event is then synced normally.

### with-nack
`(with-nack thunk) -> event`

Cancellation-aware event for use inside `choose`. `thunk` receives a nack event (fires if this branch loses) and must return an event. The nack is a condition wait-event.

### always-evt
`(always-evt value) -> event`

An event that is always ready. Fires immediately with `value`.

### never-evt
`(never-evt) -> event`

An event that is never ready. Identity element for `choose`. Syncing it directly hangs.

### make-custom-event
`(make-custom-event thunk) -> event`

Create an event from a thunk. When synced, `thunk` runs asynchronously and the event fires with its return value. Unlike `guard-evt`, the thunk *is* the operation, not a factory.

```scheme
(sync (make-custom-event (lambda () 'heartbeat)))  ; => heartbeat
```

## Channels — `(parlor channels)`

### make-channel
`(make-channel) -> channel`
`(make-channel capacity) -> channel`

Create a channel. Without arguments: rendezvous (send blocks until a receiver is ready). With a positive integer: buffered (send succeeds if buffer has space).

### send-evt
`(send-evt ch value) -> event`

An event that sends `value` on `ch`. Fires with `#f` when the send completes.

### recv-evt
`(recv-evt ch) -> event`

An event that receives from `ch`. Fires with the received value.

### send
`(send ch value) -> void`

Synchronous send. Equivalent to `(sync (send-evt ch value))`.

### recv
`(recv ch) -> value`

Synchronous receive. Equivalent to `(sync (recv-evt ch))`.

## Conditions — `(parlor conditions)`

Conditions are single-shot boolean flags. Once signalled, they stay signalled.

### make-condition
`(make-condition) -> condition`

Create an unsignalled condition.

### signal!
`(signal! cond) -> boolean`

Signal a condition. Returns `#t` if this call was the first to signal it, `#f` otherwise. All waiters are notified.

### wait-evt
`(wait-evt cond) -> event`

An event that fires when `cond` is signalled. Fires with `#t`. If already signalled, fires immediately.

### wait
`(wait cond) -> #t`

Synchronous wait. Equivalent to `(sync (wait-evt cond))`.

### make-notifier
`(make-notifier) -> notifier`

Create a notifier. Notifiers are multi-shot: each `notify!` adds one permit.

### notify!
`(notify! n) -> void`

Add one permit to notifier `n`.

### notify-evt
`(notify-evt n) -> event`

An event that fires when a permit is available on `n`. Consumes one permit. Fires with `#t`.

## Timers — `(parlor timers)`

### sleep-evt
`(sleep-evt seconds) -> event`

An event that fires after `seconds` (a real number). Fires with `#f`. If `seconds` is zero, fires immediately.

### sleep
`(sleep seconds) -> void`

Synchronous sleep. Equivalent to `(sync (sleep-evt seconds))`.

### timer-operation
`(timer-operation expiry) -> event`

An event that fires at an absolute UNIX timestamp (seconds since epoch). Fires with `#f`. If `expiry` is in the past, fires immediately.

## I/O — `(parlor io)`

### accept-evt
`(accept-evt listener) -> event`

An event that accepts a TCP connection on `listener`. Fires with a pair `(port . addr-string)` where `port` is a binary I/O port wrapping the socket.

### readable-evt
`(readable-evt port) -> event`

An event that fires when `port` has data ready to read. Fires with `port`.

### writable-evt
`(writable-evt port) -> event`

An event that fires when `port` is ready for writing. Fires with `port`.

### accept
`(accept listener) -> (port . addr-string)`

Synchronous accept. Equivalent to `(sync (accept-evt listener))`.

### connect-tcp
`(connect-tcp addr-string) -> port`

Connect to a TCP address (e.g., `"127.0.0.1:8080"`). Returns a binary I/O port. This is not an event constructor; it blocks until connected.

### listener-address
`(listener-address listener) -> string`

Return the local address of a TCP listener as a string.

## Spawn — `(parlor spawn)`

Note: `tokio/spawn` and `tokio/bind-tcp` come from `(async)`, not parlor.

### join-evt
`(join-evt future) -> event`

An event that fires when a spawned task completes. `future` is the value returned by `tokio/spawn`. Fires with the task's return value.

```scheme
(define f (tokio/spawn (lambda () (* 6 7))))
(assert (= (sync (join-evt f)) 42))
```
