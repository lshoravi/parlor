(import (rnrs) (cml) (cml channels) (cml timers) (cml conditions))

;; Stress test: many wrap operations to trigger double-decrement of
;; Procedure refcounts in BaseEvent::finalize vs visit_children.

;; Phase 1: simple wrap (BaseEvent with non-empty wrap_fns)
(do ((i 0 (+ i 1)))
    ((= i 500))
  (let ((result (sync (wrap (sleep-evt 0.0) (lambda (_) i)))))
    (assert (= result i))))
(display "phase1: 500 wrap+sync passed\n")

;; Phase 2: choose with wrap (ChoiceEvent + BaseEvent wrap_fns)
(let ((ch (make-channel 1)))
  (do ((i 0 (+ i 1)))
      ((= i 500))
    (send ch i)
    (let ((result (sync (choose (recv-evt ch)
                                (wrap (sleep-evt 10.0) (lambda (_) 'timeout))))))
      (assert (= result i))))
  (display "phase2: 500 choose+wrap passed\n"))

;; Phase 3: nested wraps (multiple Procedures in wrap_fns)
(do ((i 0 (+ i 1)))
    ((= i 200))
  (let ((result (sync (wrap (wrap (wrap (sleep-evt 0.0)
                                       (lambda (_) i))
                                  (lambda (x) (+ x 1)))
                            (lambda (x) (* x 2))))))
    (assert (= result (* (+ i 1) 2)))))
(display "phase3: 200 nested-wrap passed\n")

(display "all gc-double-decrement tests passed\n")
