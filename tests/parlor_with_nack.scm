(import (rnrs) (parlor) (parlor channels) (parlor timers) (parlor conditions)
        (parlor spawn))

;; Loser's nack fires
(let* ((nack-fired (make-condition))
       (ch (make-channel))
       (result (sync (choose
                 (with-nack
                   (lambda (nack)
                     (spawn-task (lambda ()
                       (sync nack)
                       (signal! nack-fired)))
                     (recv-evt ch)))
                 (always-evt 42)))))
  (assert (= result 42))
  (sync (sleep-evt 0.05))
  (assert (wait nack-fired))
  (display "basic-with-nack passed\n"))

;; Winner's nack does NOT fire
(let* ((nack-fired (make-condition))
       (result (sync (choose
                 (with-nack
                   (lambda (nack)
                     (spawn-task (lambda ()
                       (sync nack)
                       (signal! nack-fired)))
                     (always-evt 99)))
                 (never-evt)))))
  (assert (= result 99))
  (sync (sleep-evt 0.05))
  ;; nack-fired should not be signalled; use a timed choose to avoid blocking
  (let ((r (sync (choose
              (wrap (wait-evt nack-fired) (lambda (_) 'fired))
              (wrap (sleep-evt 0.05) (lambda (_) 'timeout))))))
    (assert (eq? r 'timeout)))
  (display "winner-no-nack passed\n"))

;; wrap distributes over with-nack
(let* ((result (sync (choose
                 (wrap (with-nack
                         (lambda (nack) (always-evt 10)))
                       (lambda (x) (* x 3)))
                 (never-evt)))))
  (assert (= result 30))
  (display "wrap-with-nack passed\n"))

(display "all with-nack tests passed\n")
