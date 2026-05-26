(library (cml conditions)
  (export make-condition signal! wait-evt wait
          make-notifier notify! notify-evt)
  (import (rnrs) (cml conditions bridge) (cml bridge))

  (define (make-condition) (%make-condition))
  (define (signal! cv) (%signal! cv))
  (define (wait-evt cv) (%wait-evt cv))
  (define (wait cv) (%sync (wait-evt cv)))
  (define (make-notifier) (%make-notifier))
  (define (notify! n) (%notify! n))
  (define (notify-evt n) (%notify-evt n)))
