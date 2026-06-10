(library (cml spawn)
  (export cml-spawn join-evt)
  (import (rnrs) (cml spawn bridge))

  (define (cml-spawn thunk) (%cml-spawn thunk))
  (define (join-evt handle) (%join-evt handle)))
