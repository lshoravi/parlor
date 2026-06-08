(library (cml)
  (export sync choose wrap guard-evt
          spawn-task run-tasks yield
          make-custom-event)
  (import (rnrs) (cml bridge) (cml tasks bridge))

  (define (sync evt) (%sync evt))
  (define (choose . evts) (apply %choose evts))
  (define (wrap evt f) (%wrap evt f))
  (define (guard-evt thunk) (%guard-evt thunk))
  (define (spawn-task thunk) (%spawn thunk))
  (define (run-tasks thunk) (%run-tasks thunk))
  (define (yield) (%yield))
  (define (make-custom-event thunk) (%make-custom-event thunk)))
