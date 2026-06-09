(library (cml timers)
  (export sleep-evt sleep timer-operation)
  (import (rnrs) (cml timers bridge) (cml bridge))

  (define (sleep-evt seconds) (%sleep-evt (inexact seconds)))
  (define (sleep seconds) (%sync (sleep-evt seconds)))
  (define (timer-operation expiry) (%timer-operation (inexact expiry))))
