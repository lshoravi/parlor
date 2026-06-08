(import (rnrs) (cml) (cml channels) (cml conditions) (cml timers)
        (prefix (async) tokio/))

;; spawn + buffered channel
(let ((ch (make-channel 1)))
  (tokio/spawn (lambda () (send ch 'hello)))
  (assert (eq? (recv ch) 'hello)))
(display "spawn-channel passed\n")

;; Rendezvous channel with spawn
(let ((ch (make-channel)))
  (tokio/spawn (lambda () (send ch 'rendezvous)))
  (assert (eq? (recv ch) 'rendezvous)))
(display "rendezvous-channel passed\n")

;; Multiple spawns
(let ((ch (make-channel 10)))
  (tokio/spawn (lambda () (send ch 1)))
  (tokio/spawn (lambda () (send ch 2)))
  (tokio/spawn (lambda () (send ch 3)))
  (let ((result (+ (recv ch) (recv ch) (recv ch))))
    (assert (= result 6))))
(display "multi-spawn-drain passed\n")

;; Notifier: spawn wakes a waiting task
(let ((n (make-notifier))
      (ch (make-channel 1)))
  (tokio/spawn
    (lambda ()
      (sync (notify-evt n))
      (send ch 'woke)))
  (notify! n)
  (assert (eq? (recv ch) 'woke)))
(display "notifier-wake passed\n")

(display "all task tests passed\n")
