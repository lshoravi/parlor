(library (cml)
  (export sync choose wrap guard
          spawn run-tasks yield
          make-custom-event)
  (import (rnrs) (cml bridge) (cml tasks bridge))

  (define (sync evt) (%sync evt))
  (define (choose . evts) (apply %choose evts))
  (define (wrap evt f) (%wrap evt f))
  (define (guard thunk) (%guard thunk))
  (define (spawn thunk) (%spawn thunk))
  (define (run-tasks thunk) (%run-tasks thunk))
  (define (yield) (%yield))
  (define (make-custom-event thunk) (%make-custom-event thunk)))
