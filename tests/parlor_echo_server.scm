(import (rnrs) (parlor) (parlor channels) (parlor io) (parlor timers)
        (parlor conditions) (parlor spawn)
        (prefix (async) tokio/))

;; --- Concurrent server exercising the full Parlor API ---
;;
;; Exercises: accept-evt, readable-evt, choose, wrap, guard-evt,
;; make-custom-event, channels (rendezvous + buffered), conditions,
;; notifiers, timers, with-nack, always-evt, never-evt, join-evt,
;; event reuse, tokio/spawn

(define listener (tokio/bind-tcp "127.0.0.1:0"))
(define addr (listener-address listener))
(define shutdown-cv (make-condition))
(define stats-ch (make-channel 100))
(define stats-done (make-notifier))

;; Client handler: accept connection, close it, report to stats
(define (handle-client client-port client-id)
  (close-port client-port)
  (send stats-ch (cons 'handled client-id)))

;; Accept loop using with-nack for cancellation awareness.
;; When shutdown wins, the accept-evt's nack fires — the server
;; knows the accept was cleanly abandoned.
(define nack-count-ch (make-channel 1))

(define (accept-loop)
  (let loop ((n 0))
    (let ((result (sync (choose
                          (with-nack
                            (lambda (nack)
                              (tokio/spawn
                                (lambda ()
                                  (sync nack)
                                  (send nack-count-ch 'nack-fired)))
                              (wrap (guard-evt (lambda () (accept-evt listener)))
                                    (lambda (pair) (cons 'client pair)))))
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

;; Stats collector using join-evt to await completion
(define stats-collector
  (tokio/spawn
    (lambda ()
      (let loop ((handled 0) (total #f))
        (if (and total (= handled total))
            (list handled total)
            (let ((msg (recv stats-ch)))
              (cond
                ((and (pair? msg) (eq? (car msg) 'handled))
                 (loop (+ handled 1) total))
                ((and (pair? msg) (eq? (car msg) 'total-accepted))
                 (loop handled (cdr msg))))))))))

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

;; 2. Accept with timeout (no client — timeout should win)
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

;; 9. Event reuse: same event object synced multiple times
(let* ((ch (make-channel 5))
       (se (send-evt ch 'ping))
       (re (recv-evt ch)))
  (sync se)
  (sync se)
  (sync se)
  (assert (eq? (sync re) 'ping))
  (assert (eq? (sync re) 'ping))
  (assert (eq? (sync re) 'ping)))
(display "event-reuse passed\n")

;; 10. always-evt and never-evt as choose identities
(let ((result (sync (choose (never-evt) (never-evt) (always-evt 'found)))))
  (assert (eq? result 'found)))
(let ((result (sync (choose (always-evt 'a) (always-evt 'b)))))
  (assert (or (eq? result 'a) (eq? result 'b))))
(display "always-never-identities passed\n")

;; 11. with-nack: RPC-with-timeout pattern
;; Simulates an RPC that takes too long; nack fires and cleans up.
(let* ((cleanup-fired (make-condition))
       (done (make-condition))
       (result
         (sync (choose
                 (with-nack
                   (lambda (nack)
                     (tokio/spawn
                       (lambda ()
                         (let ((reason (sync (choose
                                        (wrap nack (lambda (_) 'lost))
                                        (wrap (wait-evt done) (lambda (_) 'won))))))
                           (when (eq? reason 'lost)
                             (signal! cleanup-fired)))))
                     ;; "slow RPC" — will lose to the timeout
                     (wrap (sleep-evt 10.0) (lambda (_) 'rpc-result))))
                 ;; fast timeout
                 (always-evt 'timeout)))))
  (assert (eq? result 'timeout))
  (tokio/sleep 50)
  (assert (wait cleanup-fired))
  (display "rpc-with-nack-cleanup passed\n"))

;; 12. wrap distributes over with-nack
(let ((result (sync (choose
                      (wrap (with-nack (lambda (nack) (always-evt 5)))
                            (lambda (x) (* x 10)))
                      (never-evt)))))
  (assert (= result 50)))
(display "wrap-over-with-nack passed\n")

;; 13. join-evt: await spawned task completion as a CML event
(let* ((f (tokio/spawn (lambda () (* 6 7))))
       (result (sync (join-evt f))))
  (assert (= result 42)))
(display "join-evt passed\n")

;; 14. join-evt in choose: fast task beats timeout
(let* ((f (tokio/spawn (lambda () (tokio/sleep 10) 'fast)))
       (result (sync (choose (join-evt f) (sleep-evt 5)))))
  (assert (eq? result 'fast)))
(display "join-evt-choose passed\n")

;; --- Shutdown ---

(signal! shutdown-cv)
(sync (notify-evt stats-done))

;; Verify the nack from the accept-loop fired on shutdown
(let ((nack-result (sync (choose
                           (recv-evt nack-count-ch)
                           (wrap (sleep-evt 0.2) (lambda (_) 'no-nack))))))
  (assert (eq? nack-result 'nack-fired)))
(display "accept-nack-on-shutdown passed\n")

;; Use join-evt to await the stats collector instead of a raw channel
(let ((stats (sync (join-evt stats-collector))))
  (assert (= (car stats) 3))
  (assert (= (cadr stats) 3)))
(display "stats verified via join-evt: 3 clients handled\n")

(display "all echo-server tests passed\n")
