(import (rnrs) (parlor) (parlor channels) (parlor io) (parlor timers) (parlor conditions)
        (prefix (async) tokio/))

;; --- Concurrent server exercising the full CML API ---
;;
;; Exercises: accept-evt, readable-evt, choose, wrap, guard-evt,
;; make-custom-event, channels (rendezvous + buffered), conditions,
;; notifiers, timers, sleep-evt, tokio/spawn

(define listener (tokio/bind-tcp "127.0.0.1:0"))
(define addr (listener-address listener))
(define shutdown-cv (make-condition))
(define stats-ch (make-channel 100))
(define stats-done (make-notifier))

;; Client handler: accept connection, close it, report to stats
(define (handle-client client-port client-id)
  (close-port client-port)
  (send stats-ch (cons 'handled client-id)))

;; Accept loop: guard-evt lazily constructs accept-evt each iteration.
;; Choose between accepting and shutdown signal.
(define (accept-loop)
  (let loop ((n 0))
    (let ((result (sync (choose
                          (wrap (guard-evt (lambda () (accept-evt listener)))
                                (lambda (pair) (cons 'client pair)))
                          (wrap (wait-evt shutdown-cv)
                                (lambda (_) 'shutdown))))))
      (cond
        ((eq? result 'shutdown)
         (send stats-ch (cons 'total-accepted n))
         (notify! stats-done))
        (else
         (let ((client-port (cadr result))
               (client-addr (cddr result)))
           (tokio/spawn (lambda () (handle-client client-port n)))
           (loop (+ n 1))))))))

;; Stats collector
(define stats-result-ch (make-channel 1))
(tokio/spawn
  (lambda ()
    (let loop ((handled 0) (total #f))
      (if (and total (= handled total))
          (send stats-result-ch (list handled total))
          (let ((msg (recv stats-ch)))
            (cond
              ((and (pair? msg) (eq? (car msg) 'handled))
               (loop (+ handled 1) total))
              ((and (pair? msg) (eq? (car msg) 'total-accepted))
               (loop handled (cdr msg)))))))))

;; Start server
(tokio/spawn (lambda () (accept-loop)))
(tokio/sleep 50)
(display "server started\n")

;; --- Client tests ---

;; 1. Three clients connect sequentially
(let ((sock (connect-tcp addr)))
  (close-port sock))
(let ((sock (connect-tcp addr)))
  (close-port sock))
(let ((sock (connect-tcp addr)))
  (close-port sock))
(tokio/sleep 100)
(display "3 clients connected\n")

;; 2. Accept with timeout (no client connecting — timeout should win)
(let ((result (sync (choose
                      (wrap (accept-evt listener) (lambda (_) 'accepted))
                      (wrap (sleep-evt 0.01) (lambda (_) 'timeout))))))
  (assert (eq? result 'timeout)))
(display "accept-timeout passed\n")

;; 3. Custom event in choose (always ready, beats timer)
(let ((result (sync (choose
                      (make-custom-event (lambda () 'heartbeat))
                      (wrap (sleep-evt 10.0) (lambda (_) 'timeout))))))
  (assert (eq? result 'heartbeat)))
(display "custom-event-in-choose passed\n")

;; 4. Condition: signal before wait
(let ((cv (make-condition)))
  (signal! cv)
  (let ((result (sync (choose
                        (wrap (wait-evt cv) (lambda (_) 'signalled))
                        (wrap (sleep-evt 1.0) (lambda (_) 'timeout))))))
    (assert (eq? result 'signalled))))
(display "condition-in-choose passed\n")

;; 5. Notifier in choose
(let ((n (make-notifier)))
  (notify! n)
  (let ((result (sync (choose
                        (wrap (notify-evt n) (lambda (_) 'notified))
                        (wrap (sleep-evt 1.0) (lambda (_) 'timeout))))))
    (assert (eq? result 'notified))))
(display "notifier-in-choose passed\n")

;; 6. Wrap composition: wrap a wrap
(let ((result (sync (wrap (wrap (sleep-evt 0.0)
                                (lambda (_) 10))
                          (lambda (x) (* x 2))))))
  (assert (= result 20)))
(display "wrap-composition passed\n")

;; 7. Channel rendezvous via choose
(let ((ch1 (make-channel))
      (ch2 (make-channel)))
  (tokio/spawn (lambda () (send ch1 'first)))
  (tokio/spawn (lambda () (tokio/sleep 100) (send ch2 'second)))
  (let ((result (sync (choose (recv-evt ch1) (recv-evt ch2)))))
    (assert (eq? result 'first))))
(display "channel-choose passed\n")

;; 8. Buffered channel fan-in
(let ((ch (make-channel 10))
      (done-ch (make-channel 1)))
  (tokio/spawn
    (lambda ()
      (let loop ((sum 0) (count 0))
        (if (= count 5)
            (send done-ch sum)
            (loop (+ sum (recv ch)) (+ count 1))))))
  (do ((i 0 (+ i 1)))
      ((= i 5))
    (send ch i))
  (assert (= (recv done-ch) 10)))
(display "buffered-fan-in passed\n")

;; --- Shutdown ---

(signal! shutdown-cv)
(sync (notify-evt stats-done))
(let ((stats (recv stats-result-ch)))
  (assert (= (car stats) 3))
  (assert (= (cadr stats) 3)))
(display "stats verified: 3 clients handled\n")

(display "all echo-server tests passed\n")
