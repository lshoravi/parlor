(library (cml)
  (export sync choose wrap guard-evt
          make-custom-event)
  (import (rnrs) (cml bridge))

  (define (sync evt) (%sync evt))
  (define (choose . evts) (apply %choose evts))
  (define (wrap evt f) (%wrap evt f))
  (define (guard-evt thunk) (%guard-evt thunk))
  (define (make-custom-event thunk) (%make-custom-event thunk)))
