(import (rnrs) (parlor) (parlor conditions))

;; Condition: first signal returns #t, second returns #f
(let ((cv (make-condition)))
  (assert (eq? (signal! cv) #t))
  (assert (eq? (signal! cv) #f))
  (wait cv)
  (display "condition-pre-signal passed\n"))

(display "all condition tests passed\n")
