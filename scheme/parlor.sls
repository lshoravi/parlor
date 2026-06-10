(library (parlor)
  (export sync select choose wrap guard-evt
          make-custom-event always-evt never-evt with-nack)
  (import (rnrs) (parlor bridge))

  (define (sync evt) (%sync evt))
  (define (choose . evts) (apply %choose evts))
  (define (wrap evt f) (%wrap evt f))
  (define (guard-evt thunk) (%guard-evt thunk))
  (define (make-custom-event thunk) (%make-custom-event thunk))
  (define (always-evt val) (%always-evt val))
  (define (never-evt) (%never-evt))
  (define (with-nack thunk) (%with-nack thunk))
  (define (select . evts) (sync (apply choose evts))))
