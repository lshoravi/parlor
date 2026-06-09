(import (rnrs) (cml) (cml channels) (cml timers))

(let ((v (sync (always-evt 42))))
  (assert (= v 42))
  (display "always-evt passed\n"))

(let ((v (sync (wrap (always-evt 10) (lambda (x) (* x 2))))))
  (assert (= v 20))
  (display "always-evt+wrap passed\n"))

(let ((v (sync (choose (never-evt) (always-evt 99)))))
  (assert (= v 99))
  (display "never-evt+choose passed\n"))

(let* ((ch (make-channel))
       (v (sync (choose (recv-evt ch) (always-evt 7)))))
  (assert (= v 7))
  (display "always-evt-beats-blocking passed\n"))

(let ((e (always-evt 5)))
  (assert (= (sync e) 5))
  (assert (= (sync e) 5))
  (display "always-evt-reuse passed\n"))

(display "all always-never tests passed\n")
