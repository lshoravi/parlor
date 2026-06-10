(import (rnrs) (parlor) (parlor channels) (parlor conditions) (parlor timers))

(sync (sleep-evt 0.0))
(display "sync passed\n")

(let ((result (sync (choose
                      (wrap (sleep-evt 10.0) (lambda (_) 'slow))
                      (wrap (sleep-evt 0.0) (lambda (_) 'fast))))))
  (assert (eq? result 'fast))
  (display "choose passed\n"))

(let ((result (sync (wrap (sleep-evt 0.0) (lambda (_) 42)))))
  (assert (= result 42))
  (display "wrap passed\n"))

(let ((result (sync (guard-evt (lambda () (sleep-evt 0.0))))))
  (display "guard passed\n"))

(let ((ch (make-channel 1)))
  (send ch 'buf)
  (assert (eq? (recv ch) 'buf))
  (display "make-channel-buffered passed\n"))

(let ((ch (make-channel 1)))
  (send ch 'convenient)
  (assert (eq? (recv ch) 'convenient))
  (display "send-recv passed\n"))

(let ((ch (make-channel 1)))
  (sync (send-evt ch 'via-evt))
  (let ((result (sync (recv-evt ch))))
    (assert (eq? result 'via-evt)))
  (display "send-evt-recv-evt passed\n"))

(let ((cv (make-condition)))
  (assert (eq? (signal! cv) #t))
  (assert (eq? (signal! cv) #f))
  (display "make-condition-signal passed\n"))

(let ((cv (make-condition)))
  (signal! cv)
  (sync (wait-evt cv))
  (display "wait-evt passed\n"))

(let ((cv (make-condition)))
  (signal! cv)
  (wait cv)
  (display "wait passed\n"))

(let ((n (make-notifier)))
  (notify! n)
  (sync (notify-evt n))
  (display "make-notifier-notify-evt passed\n"))

(sync (sleep-evt 0.001))
(display "sleep-evt passed\n")

(sleep 0.001)
(display "sleep passed\n")

(let ((result (sync (make-custom-event (lambda () 'custom-val)))))
  (assert (eq? result 'custom-val))
  (display "make-custom-event passed\n"))

(display "all api-coverage tests passed\n")
