# Writer Worker Spec

## Goal
`WriterWorker` drains missing blocks from the internal queue and sends blind write commands to the active backup driver.

## Inputs and dependencies
- Input items: `(hash, bytes)` blocks via `enqueue(hash, bytes)`.
- Per-shard readiness gate:
- `isShardReady(shardKey)`
- `waitForAnyShardReady()`
- Write execution callback:
- `scheduleWrite(hash, bytes)`
- Concurrency limit callback:
- `onConcurrencyLimitChanged(concurrency)`
- Metrics callback:
- `onMetrics(queuedBytes, inFlightBytes, driverBufferedBytes)`
- Physical progress callback:
- `handlePhysicalBytes(bytes)`

## Queue model
- Queue is organized by shard key (`hash[0..4)` when available).
- Internal structure:
- per-shard block buckets
- shard round-robin order list
- The worker keeps fair scheduling across shards by rotating shard keys.

## Scheduling behavior
- Continuous loop runs until:
- `signalDone()` is called
- local queue is empty
- in-flight writes are empty
- If queue is empty but writes are still in flight: drain completed writes.
- If queue has data:
- iterate shard queue
- schedule writes while `inFlight < targetConcurrentWrites`
- honor per-block `retryAt` delays before rescheduling failed writes
- If nothing could be scheduled:
- if in-flight exists: drain one completed write
- else wait for shard-ready signal (bounded by short timeout polling)

## Retry and cooldown ownership
- Writer owns retry policy for `scheduleWrite` failures.
- Failed writes are re-queued with exponential backoff.
- After a failure, writer lowers `targetConcurrentWrites` (minimum `1`) and notifies driver via `onConcurrencyLimitChanged`.
- Writer increases concurrency in cooldown steps until `maxConcurrentWrites` is restored.

## Backpressure behavior
- Backlog bytes = `queuedBytes + inFlightBytes + driverBufferedBytes()`.
- Backpressure starts when backlog exceeds `backlogLimitBytes`.
- Backpressure clears when backlog is at or below `backlogClearBytes`.
- `waitForBackpressureClear()` blocks upstream readers while backpressure is active.

## Timeouts and errors
- A write failure is retried until `maxRetryAttempts` is reached.
- When retry budget is exhausted, worker sets error state.
- On error:
- writer loop aborts
- waiters are released
- later callers see error via `throwIfError()`

## Metrics and logging
- Metrics are reported whenever queue/in-flight counters change.
- Queue stats are logged periodically (`written % 512 == 0`, throttled by `logInterval`).
- Optional loop debug logs include:
- reason (`scheduled`, `drain-inflight`, `wait-shard-ready`)
- queued blocks/bytes
- in-flight bytes
- current/maximum concurrency

## Output guarantees
- Every enqueued block is attempted at least once, and retried until success or retry budget exhaustion.
- Physical bytes are counted only when a write future completes successfully.
- Worker does not reorder blocks within the same shard bucket beyond FIFO dequeue behavior.
