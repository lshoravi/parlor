(library (parlor spawn)
  (export spawn-task join-evt)
  (import (rnrs) (parlor spawn bridge) (prefix (async) async/))

  (define (spawn-task thunk) (async/spawn thunk))
  (define (join-evt future) (%join-evt future)))
