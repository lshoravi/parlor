(import (rnrs) (cml) (cml channels))

(define (rpc-fib n)
  (if (< n 2)
      n
      (run-tasks
        (lambda ()
          (let ((ch1 (make-channel 1))
                (ch2 (make-channel 1)))
            (spawn-task (lambda () (send ch1 (rpc-fib (- n 1)))))
            (spawn-task (lambda () (send ch2 (rpc-fib (- n 2)))))
            (+ (recv ch1) (recv ch2)))))))

(let ((result (rpc-fib 15)))
  (assert (= result 610))
  (display "rpc-fib passed\n"))

(run-tasks
  (lambda ()
    (let ((req-ch (make-channel 5))
          (done-ch (make-channel 5)))

      (spawn-task
        (lambda ()
          (let loop ((i 0))
            (when (< i 500)
              (let ((resp-ch (recv req-ch)))
                (send resp-ch 'pong)
                (loop (+ i 1)))))))

      (do ((c 0 (+ c 1)))
          ((= c 5))
        (spawn-task
          (lambda ()
            (do ((r 0 (+ r 1)))
                ((= r 100))
              (let ((resp (make-channel 1)))
                (send req-ch resp)
                (assert (eq? (recv resp) 'pong))))
            (send done-ch 'ok))))

      (do ((c 0 (+ c 1)))
          ((= c 5))
        (recv done-ch)))))
(display "pingpong passed\n")

(let ((result
        (run-tasks
          (lambda ()
            (let ((collector (make-channel 1000)))
              (do ((p 0 (+ p 1)))
                  ((= p 20))
                (let ((pid p))
                  (spawn-task
                    (lambda ()
                      (do ((i 0 (+ i 1)))
                          ((= i 50))
                        (send collector pid))))))
              (let loop ((i 0) (sum 0))
                (if (= i 1000)
                    sum
                    (loop (+ i 1) (+ sum (recv collector))))))))))
  (assert (= result (* 50 (/ (* 19 20) 2))))
  (display "fan-in passed\n"))

(display "all stress-channels tests passed\n")
