(import (rnrs) (cml) (cml channels) (cml timers) (cml spawn) (async))

;; Basic: spawn returns future, join-evt fires with result
(let* ((f (spawn (lambda () 42)))
       (v (sync (join-evt f))))
  (assert (= v 42))
  (display "basic-join passed\n"))

;; join-evt in choose: task completes before timeout
(let* ((f (spawn (lambda () (sync (sleep-evt 0.01)) 99)))
       (v (sync (choose (join-evt f) (sleep-evt 1)))))
  (assert (= v 99))
  (display "join-beats-timeout passed\n"))

;; join-evt in choose: always-evt wins over slow task
(let* ((f (spawn (lambda () (sync (sleep-evt 10)) 0)))
       (v (sync (choose (join-evt f) (always-evt 7)))))
  (assert (= v 7))
  (display "timeout-beats-join passed\n"))

(display "all spawn-join tests passed\n")
