(library (cml spawn)
  (export join-evt)
  (import (rnrs) (cml spawn bridge))

  (define (join-evt future) (%join-evt future)))
