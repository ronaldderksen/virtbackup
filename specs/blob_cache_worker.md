# Blob Cache Worker Spec

## Goal
The `blob_cache_worker` manages the blob directory cache during a backup run.

## Input
- Hashes come directly from `hashblocks`.
- Every incoming hash is placed into an independent queue.
- In parallel, an exists queue receives the same hashes for existing/missing decisions.

## Worker lifecycle
1. At worker startup:
- Set `writeReady=false`.
- Perform one top-level scan of `blobs` via the active driver (`00..ff` shards).
- Determine missing shard directories and create them via the driver (max 256).
- Set `writeReady=true` only after all create calls finished successfully.

2. Then process the queue:
- Process incoming items immediately.
- The worker does not wait for other components; it only processes queue content.

## Cache behavior during processing
For a hash like `aa...`:
- Scan `aa` only if `aa` is not already in the cache.
- Use the full `list blobs/aa` result as the source of truth for existing/missing in `aa`.
- If later hashes with the same `aa` arrive, always use the cache.
- Per backup run: scan a directory at most once, then never scan it again.

## Directory creation behavior
- The blob cache sends commands to the driver to create directories.
- Based on its own cache, the blob cache determines:
- which directories already exist / were already created
- which directories still need to be created

## Writer dependency contract
- Writer and blob cache are independent except for one signal: `writeReady`.
- Writer waits only for `writeReady=true`.
- No shard-ready dependency exists between writer and blob cache.

## Existing/Missing basis
- Existing/missing is decided by a separate exists worker.
- Existing hashes are handled immediately and are not sent to SFTP or writer.
- Only missing hashes are sent to SFTP and writer.
