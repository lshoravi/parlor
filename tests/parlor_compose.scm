(import (rnrs) (parlor) (parlor timers) (parlor conditions))

;; wrap transforms the result
(let ((result (sync (wrap (sleep-evt 0.0) (lambda (_) 'wrapped)))))
  (assert (eq? result 'wrapped))
  (display "wrap passed\n"))

;; choose picks the faster event
(let ((result (sync (choose
                      (wrap (sleep-evt 10.0) (lambda (_) 'slow))
                      (wrap (sleep-evt 0.0) (lambda (_) 'fast))))))
  (assert (eq? result 'fast))
  (display "choose-fast-wins passed\n"))

;; choose with pre-signalled condition hits try path
(let ((cv (make-condition)))
  (signal! cv)
  (let ((result (sync (choose
                         (wrap (wait-evt cv) (lambda (_) 'condition))
                         (wrap (sleep-evt 10.0) (lambda (_) 'timeout))))))
    (assert (eq? result 'condition))
    (display "choose-try-path passed\n")))

(let ((result (sync (guard-evt (lambda () (sleep-evt 0.0))))))
  (display "guard passed\n"))

;; wrap(wrap(ev, g), f) = wrap(ev, f . g)
(let* ((ev (always-evt 3))
       (nested (sync (wrap (wrap ev (lambda (x) (* x 10))) (lambda (x) (+ x 1)))))
       (composed (sync (wrap ev (lambda (x) (+ (* x 10) 1))))))
  (assert (= nested composed))
  (assert (= nested 31))
  (display "wrap-composition-law passed\n"))

;; wrap(choose(ev1, ev2), f) = choose(wrap(ev1, f), wrap(ev2, f))
(let* ((f (lambda (x) (* x 2)))
       (ev1 (always-evt 5))
       (ev2 (always-evt 7))
       (lhs (sync (wrap (choose ev1 ev2) f)))
       (rhs (sync (choose (wrap ev1 f) (wrap ev2 f)))))
  (assert (or (= lhs 10) (= lhs 14)))
  (assert (or (= rhs 10) (= rhs 14)))
  (display "wrap-distribution-law passed\n"))

(display "all composition tests passed\n")
