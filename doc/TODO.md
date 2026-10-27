# TODO

## High priority
1. Add SSH host key verification to prevent MITM.

## Medium priority
1. Remove debug hardcoded agent URL; make configurable via settings/UI.
3. Improve hashblocks streaming queue (use ListQueue; avoid O(n) removeAt(0); adjust queue limits).
4. Extend restore XML parsing to handle non-file sources (block devices, protocol sources) and fail clearly.
5. Google Drive: persist folder-id caching to reduce Drive API calls.
6. Google Drive: lazy index cache per shard (avoid loading all indexes at startup).
7. Google Drive: parallel pack uploads via a queue (bounded concurrency).

## Low priority
1. Extend endpoint and manifest coverage beyond the existing failure, rollback, pipeline, certificate, and token-refresh regression tests.
