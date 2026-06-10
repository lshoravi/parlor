(import (rnrs) (parlor) (parlor timers))

(sleep 0.0)
(display "zero-sleep passed\n")

(sleep 0.01)
(display "short-sleep passed\n")

(sync (sleep-evt 0.01))
(display "sync-sleep-evt passed\n")

(display "all basic tests passed\n")
