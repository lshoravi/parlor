(import (rnrs) (cml) (cml channels) (cml timers) (cml conditions)
        (prefix (async) tokio/))

(sync (sleep-evt 0.0))
(display "1\n")
(sync (sleep-evt 0.001))
(display "2\n")

(sync (choose (wrap (sleep-evt 10.0) (lambda (_) 'slow))
              (wrap (sleep-evt 0.0) (lambda (_) 'fast))))
(display "3\n")

(let ((ch (make-channel 1)))
  (tokio/spawn (lambda () (send ch 'x)))
  (recv ch))
(display "4 spawn+recv passed\n")

(do ((i 0 (+ i 1)))
    ((= i 50))
  (sync (sleep-evt 0.0)))
(display "5 50x sleep done\n")

(let ((ch (make-channel 1)))
  (tokio/spawn (lambda () (send ch 'y)))
  (recv ch))
(display "6 spawn+recv after accumulation passed\n")

(display "all sleep+spawn tests passed\n")
