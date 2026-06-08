(import (rnrs) (cml) (cml channels) (cml timers) (cml conditions))

;; Reproduce: many CML sync operations followed by spawn-task.
;; If GC corruption from double-decrement bleeds into runtime state,
;; the spawn-task may deadlock or crash.

;; Warm up: many sync+wrap to accumulate GC corruption
(do ((i 0 (+ i 1)))
    ((= i 300))
  (sync (wrap (sleep-evt 0.0) (lambda (_) i))))
(display "warmup: 300 sync+wrap done\n")

;; Now try spawn-task: should still work if GC is healthy
(let ((ch (make-channel 1)))
  (run-tasks
    (lambda ()
      (spawn-task (lambda () (send ch 'alive)))
      (let ((result (recv ch)))
        (assert (eq? result 'alive))
        (display "spawn-after-sync passed\n")))))

;; More wraps, then another spawn
(do ((i 0 (+ i 1)))
    ((= i 300))
  (sync (wrap (sleep-evt 0.0) (lambda (_) i))))

(let ((ch (make-channel 1)))
  (run-tasks
    (lambda ()
      (spawn-task (lambda () (send ch 42)))
      (let ((result (recv ch)))
        (assert (= result 42))
        (display "second-spawn passed\n")))))

(display "all gc-then-spawn tests passed\n")
