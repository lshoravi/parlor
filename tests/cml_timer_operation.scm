(import (rnrs) (cml) (cml timers))

;; timer-operation takes an absolute UNIX timestamp (seconds since epoch).

;; Past timestamp (epoch) should fire immediately.
(let ((result (sync (wrap (timer-operation 0.0) (lambda (_) 'fired)))))
  (assert (eq? result 'fired))
  (display "timer-operation-past passed\n"))

;; A very old timestamp should also fire immediately.
(let ((result (sync (wrap (timer-operation 1000000.0) (lambda (_) 'old)))))
  (assert (eq? result 'old))
  (display "timer-operation-old passed\n"))

;; Past timestamp should beat a long sleep in choose.
(let ((result (sync (choose
                      (wrap (timer-operation 0.0) (lambda (_) 'timer))
                      (wrap (sleep-evt 10.0) (lambda (_) 'slow))))))
  (assert (eq? result 'timer))
  (display "timer-operation-beats-sleep passed\n"))

;; Far-future timestamp should lose to a short sleep.
;; Year 2100 ≈ 4102444800 seconds since epoch.
(let ((result (sync (choose
                      (wrap (timer-operation 4102444800.0) (lambda (_) 'timer))
                      (wrap (sleep-evt 0.01) (lambda (_) 'sleep))))))
  (assert (eq? result 'sleep))
  (display "timer-operation-far-future-loses passed\n"))

(display "all timer-operation tests passed\n")
