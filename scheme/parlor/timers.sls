(library (parlor timers)
  (export sleep-evt sleep timer-operation)
  (import (rnrs) (parlor timers bridge) (parlor bridge))

  (define (sleep-evt seconds) (%sleep-evt (inexact seconds)))
  (define (sleep seconds) (%sync (sleep-evt seconds)))
  (define (timer-operation expiry) (%timer-operation (inexact expiry))))
