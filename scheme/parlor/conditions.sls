(library (parlor conditions)
  (export make-condition signal! wait-evt wait
          make-notifier notify! notify-evt)
  (import (rnrs) (parlor conditions bridge) (parlor bridge))

  (define (make-condition) (%make-condition))
  (define (signal! cv) (%signal! cv))
  (define (wait-evt cv) (%wait-evt cv))
  (define (wait cv) (%sync (wait-evt cv)))
  (define (make-notifier) (%make-notifier))
  (define (notify! n) (%notify! n))
  (define (notify-evt n) (%notify-evt n)))
