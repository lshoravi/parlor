(import (rnrs) (parlor) (parlor channels) (parlor timers) (parlor conditions)
        (prefix (async) tokio/))

;; Warm up: many sync+wrap to accumulate GC pressure
(do ((i 0 (+ i 1)))
    ((= i 100))
  (sync (wrap (sleep-evt 0.0) (lambda (_) i))))
(display "warmup: 100 sync+wrap done\n")

;; Now try spawn: should still work if GC is healthy
(let ((ch (make-channel 1)))
  (tokio/spawn (lambda () (send ch 'alive)))
  (let ((result (recv ch)))
    (assert (eq? result 'alive))
    (display "spawn-after-sync passed\n")))

;; More wraps, then another spawn
(do ((i 0 (+ i 1)))
    ((= i 100))
  (sync (wrap (sleep-evt 0.0) (lambda (_) i))))

(let ((ch (make-channel 1)))
  (tokio/spawn (lambda () (send ch 42)))
  (let ((result (recv ch)))
    (assert (= result 42))
    (display "second-spawn passed\n")))

(display "all gc-then-spawn tests passed\n")
