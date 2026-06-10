(import (rnrs) (parlor) (parlor channels))

;; Buffered channel: send doesn't block when buffer has space
(let ((ch (make-channel 2)))
  (send ch 'a)
  (send ch 'b)
  (assert (eq? (recv ch) 'a))
  (assert (eq? (recv ch) 'b))
  (display "buffered-channel passed\n"))

;; Buffered channel with single capacity
(let ((ch (make-channel 1)))
  (send ch 42)
  (assert (eqv? (recv ch) 42))
  (display "buffered-single passed\n"))

(display "all channel tests passed\n")
