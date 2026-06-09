(import (rnrs) (cml) (cml io) (cml timers) (prefix (async) tokio/))

(define listener (tokio/bind-tcp "127.0.0.1:0"))
(define addr (listener-address listener))

;; Test 1: accept with a connecting client
(tokio/spawn
  (lambda ()
    (let ((sock (connect-tcp addr)))
      (close-port sock))))

(let ((pair (accept listener)))
  (assert (pair? pair))
  (close-port (car pair)))
(display "accept passed\n")

;; Test 2: accept-evt
(tokio/spawn
  (lambda ()
    (let ((sock (connect-tcp addr)))
      (close-port sock))))

(let ((pair (sync (accept-evt listener))))
  (assert (pair? pair))
  (close-port (car pair)))
(display "accept-evt passed\n")

;; Test 3: accept-evt in choose (connection arrives)
(tokio/spawn
  (lambda ()
    (tokio/sleep 50)
    (let ((sock (connect-tcp addr)))
      (close-port sock))))

(let ((result (sync (choose
                      (wrap (accept-evt listener) (lambda (pair) 'connected))
                      (wrap (sleep-evt 5.0) (lambda (_) 'timeout))))))
  (assert (eq? result 'connected)))
(display "accept-evt+choose passed\n")

;; Test 4: accept-evt in choose (timeout wins)
(let ((result (sync (choose
                      (wrap (accept-evt listener) (lambda (pair) 'connected))
                      (wrap (sleep-evt 0.01) (lambda (_) 'timeout))))))
  (assert (eq? result 'timeout)))
(display "accept-evt-timeout passed\n")

(display "all io tests passed\n")
