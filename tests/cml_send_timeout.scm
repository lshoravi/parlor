(import (rnrs) (cml) (cml channels) (cml timers) (prefix (async) tokio/))

;; Rendezvous channel send with no receiver: timer should win via choose.
(let ((ch (make-channel)))
  (let ((result (sync (choose
                         (wrap (send-evt ch 'msg) (lambda (_) 'sent))
                         (wrap (sleep-evt 0.05) (lambda (_) 'timeout))))))
    (assert (eq? result 'timeout))
    (display "send-timeout passed\n")))

;; Verify the channel is still usable after a timed-out send.
(let ((ch (make-channel))
      (result-ch (make-channel 1)))
  ;; First: a send that times out (no receiver).
  (sync (choose
          (wrap (send-evt ch 'abandoned) (lambda (_) 'sent))
          (wrap (sleep-evt 0.05) (lambda (_) 'timeout))))
  ;; Now do a proper rendezvous to prove the channel still works.
  (tokio/spawn (lambda ()
                 (let ((val (recv ch)))
                   (send result-ch val))))
  (send ch 'after-timeout)
  (assert (eq? (recv result-ch) 'after-timeout))
  (display "channel-reuse-after-timeout passed\n"))

(display "all send-timeout tests passed\n")
