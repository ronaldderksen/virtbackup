# Dedup (agent, current implementation)

Status: implemented. This document describes the dedup storage and manifest format currently used by the agent.

## Goal
- Content-addressed blob store (SHA-256) shared across backups.
- Per-VM-disk restore points via manifests.
- No encryption (for now).
- Implemented behind a storage driver (`filesystem`, `gdrive`, `dummy`).

## Block/Hash
- Block size: **1 MiB** (`1024 * 1024` bytes).
- Hash: SHA-256 over the raw block bytes.
- Blob filename: hex SHA-256.
- Directory sharding: `blobs/ab/<hash>` where `ab=hash[0..1]`.
- Zero blocks are not stored as blobs; they are recorded as `ZERO` runs in the manifest.

## Storage layout (filesystem driver)
The filesystem driver uses `backup.base_path` as its base path and creates and uses a `VirtBackup` folder inside that path:
```
backup.base_path/
  VirtBackup/
    blobs/
      ab/<sha256>
    manifests/
      <serverId>/
        <vmName>/
          <timestamp>__domain.xml
          <timestamp>__<diskId>.chain        (only if there is a backing chain)
          <diskId>/
            <timestamp>.manifest.gz
    tmp/
```

The Google Drive driver uses the same logical layout (manifests + `blobs/ab/<hash>`), but syncs to Drive and keeps a local cache.

## Manifest format (text, gzipped)
One manifest is written per disk and stored as `.manifest.gz`.

Headers:
```
version: 1
block_size: 1048576
server_id: <serverId>
vm_name: <vmName>
disk_id: <diskId>
source_path: </remote/path/to/disk.qcow2>
file_size: <bytes>          (present if known)
timestamp: 2026-02-01T12:34:56.000Z
blocks:
```

Blocks (0-based):
```
0 -> <sha256>
1 -> <sha256>
6 -> ZERO
100-120 -> ZERO
```

## Backup flow (agent)
The agent creates a per-disk manifest and appends entries while the backup is running.

There are two paths:
1. **Hashblocks path (primair)**
   - The `hashblocks` binary is uploaded to the remote host and executed there.
   - The agent consumes block hashes (and ZERO runs) from the remote output.
   - For hashes: it checks blob existence via the driver; only missing blocks are fetched via SFTP range reads and written as blobs.
   - Backpressure: the agent sends `LIMIT` updates to hashblocks based on writer backlog.
2. **Stream fallback path**
   - If `hashblocks` is not available, the agent streams the entire disk file via SFTP.
   - The agent chunks into 1 MiB blocks, hashes locally, and writes only missing blobs.

## Restore flow (agent)
1. The agent resolves the correct `*.manifest.gz` per disk.
2. The manifest is read; for each block:
   - `ZERO` means "write zero bytes" for the block range.
   - A hash means "read blob `blobs/ab/<hash>` and write it to the output".
3. The last (tail) block length can be smaller than 1 MiB based on `file_size`.

## Chain metadata (optional)
If a disk has a backing chain, the agent writes a `*.chain` file in the VM directory listing chain entries (paths + disk_id). This is used to map restores to the correct manifest(s).

## Concurrency / safety
- Blob writes: atomic "temp -> rename" per driver (filesystem and gdrive) to avoid races and partially-written blobs.
- Manifests: during backup an `.inprogress` file is written; on commit the manifest is gzipped and the plain manifest is deleted.

## Out of scope (intentionally)
- Garbage collection / refcounting for blobs (not implemented yet).
- Encryption / blob compression (not implemented; only manifests are gzipped).
