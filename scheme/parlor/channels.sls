(library (parlor channels)
  (export make-channel send-evt recv-evt send recv)
  (import (rnrs) (parlor channels bridge) (parlor bridge))

  (define make-channel
    (case-lambda
      (() (%make-rendezvous-channel))
      ((capacity) (%make-buffered-channel capacity))))

  (define (send-evt ch msg) (%send-evt ch msg))
  (define (recv-evt ch) (%recv-evt ch))
  (define (send ch msg) (%sync (send-evt ch msg)))
  (define (recv ch) (%sync (recv-evt ch))))
