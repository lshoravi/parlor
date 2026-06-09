(library (cml io)
  (export accept-evt readable-evt writable-evt
          accept connect-tcp listener-address)
  (import (rnrs) (cml) (cml io bridge))

  (define (accept-evt listener) (%accept-evt listener))
  (define (readable-evt port) (%readable-evt port))
  (define (writable-evt port) (%writable-evt port))
  (define (accept listener) (sync (accept-evt listener)))
  (define (connect-tcp addr) (%connect-tcp addr))
  (define (listener-address listener) (%listener-address listener)))
