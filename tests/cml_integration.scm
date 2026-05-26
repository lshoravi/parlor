(import (rnrs) (cml) (cml channels) (cml conditions) (cml timers))

;; Simulated paint loop: notifier wakes paint task, event channel dispatches
(let ((result
        (run-tasks
          (lambda ()
            (let ((repaint (make-notifier))
                  (events (make-channel 3))
                  (output (make-channel 10)))

              ;; Paint task: wait for notification, report, loop 3 times
              (spawn-task
                (lambda ()
                  (let loop ((count 0))
                    (if (= count 3)
                        (send output (list 'paint-done count))
                        (begin
                          (sync (notify-evt repaint))
                          (send output (list 'painted count))
                          (loop (+ count 1)))))))

              ;; Event dispatch: receive events, signal repaint
              (spawn-task
                (lambda ()
                  (let loop ((i 0))
                    (when (< i 3)
                      (recv events)
                      (notify! repaint)
                      (loop (+ i 1))))))

              ;; Driver: send 3 events, collect results
              (send events 'click)
              (send events 'move)
              (send events 'click)

              ;; Collect all outputs
              (let loop ((results '()) (i 0))
                (if (= i 4)
                    (reverse results)
                    (loop (cons (recv output) results)
                          (+ i 1)))))))))
  (display result)
  (newline)
  (display "paint-loop integration passed\n"))

;; Composable timeout pattern
(let ((result
        (run-tasks
          (lambda ()
            (let ((ch (make-channel)))
              ;; Nobody sends on ch, so timeout wins
              (sync (choose
                      (wrap (recv-evt ch) (lambda (m) (list 'msg m)))
                      (wrap (sleep-evt 0.05) (lambda (_) '(timeout))))))))))
  (assert (equal? result '(timeout)))
  (display "timeout-pattern passed\n"))

;; Custom event in choose
(let ((result
        (run-tasks
          (lambda ()
            (sync (choose
                    (wrap (make-custom-event (lambda () (sleep 10.0) 'slow))
                          (lambda (x) x))
                    (wrap (sleep-evt 0.0) (lambda (_) 'fast))))))))
  (assert (eq? result 'fast))
  (display "custom-event-in-choose passed\n"))

(display "all integration tests passed\n")
