(library (parlor io)
  (export accept-evt readable-evt writable-evt
          accept connect-tcp bind-tcp listener-address)
  (import (rnrs) (parlor) (parlor io bridge) (prefix (async) async/))

  (define (accept-evt listener) (%accept-evt listener))
  (define (readable-evt port) (%readable-evt port))
  (define (writable-evt port) (%writable-evt port))
  (define (accept listener) (sync (accept-evt listener)))
  (define (connect-tcp addr) (%connect-tcp addr))
  (define (bind-tcp addr) (async/bind-tcp addr))
  (define (listener-address listener) (%listener-address listener)))
