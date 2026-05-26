(import (rnrs) (cml) (cml channels) (cml conditions) (cml timers))

;; run-tasks returns thunk's value
(assert (= (run-tasks (lambda () 42)) 42))
(display "run-tasks-return passed\n")

;; spawn + buffered channel
(assert (eq?
  (run-tasks
    (lambda ()
      (let ((ch (make-channel 1)))
        (spawn (lambda () (send ch 'hello)))
        (recv ch))))
  'hello))
(display "spawn-channel passed\n")

;; Rendezvous channel with spawn
(assert (eq?
  (run-tasks
    (lambda ()
      (let ((ch (make-channel)))
        (spawn (lambda () (send ch 'rendezvous)))
        (recv ch))))
  'rendezvous))
(display "rendezvous-channel passed\n")

;; Multiple spawns drain before run-tasks returns
(let ((result
        (run-tasks
          (lambda ()
            (let ((ch (make-channel 10)))
              (spawn (lambda () (send ch 1)))
              (spawn (lambda () (send ch 2)))
              (spawn (lambda () (send ch 3)))
              (+ (recv ch) (recv ch) (recv ch)))))))
  (assert (= result 6))
  (display "multi-spawn-drain passed\n"))

;; Notifier: spawn wakes a waiting task
(assert (eq?
  (run-tasks
    (lambda ()
      (let ((n (make-notifier))
            (ch (make-channel 1)))
        (spawn
          (lambda ()
            (sync (notify-evt n))
            (send ch 'woke)))
        (notify! n)
        (recv ch))))
  'woke))
(display "notifier-wake passed\n")

(display "all task tests passed\n")
