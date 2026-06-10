(import (rnrs) (parlor) (parlor channels) (parlor timers) (prefix (async) tokio/))

;; Regression test for commit 171801ab:
;; guard-evt inside choose where all try_fns return None, forcing
;; the block path through guard_sync_choice.

;; A rendezvous channel with no data ready means recv-evt's try_fn
;; returns None. The guard-evt wraps a choose of two such recv-evts,
;; so the inner choose also falls through to the block path.
;; A timer alternative in the outer choose should still win.

(let ((ch1 (make-channel))
      (ch2 (make-channel)))
  (let ((result (sync (choose
                         (guard-evt (lambda ()
                                      (choose (recv-evt ch1) (recv-evt ch2))))
                         (wrap (sleep-evt 0.05) (lambda (_) 'timeout))))))
    (assert (eq? result 'timeout))
    (display "guard-block-path-timeout passed\n")))

;; Same setup but a sender unblocks one of the inner channels.
;; The guard-evt block path should deliver the message.
(let ((ch1 (make-channel))
      (ch2 (make-channel)))
  (tokio/spawn (lambda () (tokio/sleep 50) (send ch1 'from-ch1)))
  (let ((result (sync (choose
                         (guard-evt (lambda ()
                                      (choose (recv-evt ch1) (recv-evt ch2))))
                         (wrap (sleep-evt 5.0) (lambda (_) 'timeout))))))
    (assert (eq? result 'from-ch1))
    (display "guard-block-path-recv passed\n")))

;; guard-evt returning a single event (not a choice) that also
;; requires the block path.
(let ((ch (make-channel)))
  (tokio/spawn (lambda () (tokio/sleep 30) (send ch 'delivered)))
  (let ((result (sync (choose
                         (guard-evt (lambda () (recv-evt ch)))
                         (wrap (sleep-evt 5.0) (lambda (_) 'timeout))))))
    (assert (eq? result 'delivered))
    (display "guard-block-path-single passed\n")))

(display "all guard-block-path tests passed\n")
