(library (parlor spawn)
  (export join-evt)
  (import (rnrs) (parlor spawn bridge))

  (define (join-evt future) (%join-evt future)))
