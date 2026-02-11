# Blob Cache Worker Spec

## Goal
The `blob_cache_worker` manages the blob directory cache during a backup run.

## Input
- Hashes come directly from `hashblocks`.
- Every incoming hash is placed into an independent queue.
- In parallel, an exists queue receives the same hashes for existing/missing decisions.

## Worker lifecycle
1. At worker startup:
- Perform an initial scan of `blobs` via the active driver.

2. Then process the queue:
- Process incoming items immediately.
- The worker does not wait for other components; it only processes queue content.

## Cache behavior during processing
For a hash like `aa/bb/cc...`:
- Scan `aa` only if `aa` is not already in the cache.
- Scan `aa/bb` only if `aa/bb` is not already in the cache.
- If later hashes with the same `aa` or `aa/bb` arrive, always use the cache.
- Per backup run: scan a directory at most once, then never scan it again.

## Directory creation behavior
- The blob cache sends commands to the driver to create directories.
- Based on its own cache, the blob cache determines:
- which directories already exist / were already created
- which directories still need to be created

## Shard-ready contract for writer
- The writer asks: `is shard aa/bb ready?`
- The answer is strictly `yes` or `no`.
- `yes` is returned only when `aa` and `aa/bb` are present in the cache.

## Existing/Missing basis
- Existing/missing is decided by a separate exists worker.
- The exists worker waits for shard-ready (`aa/bb`) before checking `blobExists`.
