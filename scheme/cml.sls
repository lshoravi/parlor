(library (cml)
  (export sync choose wrap guard)
  (import (rnrs) (cml bridge))

  (define (sync evt) (%sync evt))
  (define (choose . evts) (apply %choose evts))
  (define (wrap evt f) (%wrap evt f))
  (define (guard thunk) (%guard thunk)))
