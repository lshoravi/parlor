(import (rnrs) (cml) (cml timers) (cml conditions) (cml bridge))

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

;; guard produces event at sync time (use %guard to avoid name clash with rnrs guard)
(let ((result (sync (%guard (lambda () (sleep-evt 0.0))))))
  (display "guard passed\n"))

(display "all composition tests passed\n")
