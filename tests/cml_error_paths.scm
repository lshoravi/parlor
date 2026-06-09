(import (rnrs) (cml) (cml channels) (cml timers))

;; Test 1: choose with no alternatives should raise an error.
(guard (exn (#t (display "choose-no-args error passed\n")))
  (sync (choose))
  (assert #f))

;; Test 2: sync with a non-event should raise an error.
(guard (exn (#t (display "sync-non-event error passed\n")))
  (sync 42)
  (assert #f))

;; Test 3: send-evt with wrong type (not a channel) should raise an error.
(guard (exn (#t (display "send-evt-wrong-type error passed\n")))
  (send-evt "not-a-channel" 'msg)
  (assert #f))

;; Test 4: recv-evt with wrong type should raise an error.
(guard (exn (#t (display "recv-evt-wrong-type error passed\n")))
  (recv-evt 123)
  (assert #f))

;; Test 5: buffered channel with capacity 0 should raise an error.
(guard (exn (#t (display "buffered-cap-zero error passed\n")))
  (make-channel 0)
  (assert #f))

;; Test 6: wrap with a non-event should raise an error.
(guard (exn (#t (display "wrap-non-event error passed\n")))
  (wrap 'not-an-event (lambda (x) x))
  (assert #f))

(display "all error-path tests passed\n")
