# Dedup (agent, current implementation)

Status: implemented. This document describes the dedup storage and manifest format currently used by the agent.

## Goal
- Content-addressed blob store (SHA-256) shared across backups.
- Per-VM restore points via manifests (single manifest file containing all disks).
- No encryption (for now).
- Implemented behind a storage driver (`filesystem`, `gdrive`, `dummy`).

## Block/Hash
- Block size: configurable via `agent.yaml` key `blockSizeMB` (allowed: `1`, `2`, `4`, `8`; default persisted on startup: `1`).
- Hash: SHA-256 over the raw block bytes.
- Blob filename: hex SHA-256.
- Directory sharding: `blobs/<blockSizeMB>/ab/<hash>` where `ab=hash[0..1]`.
- Zero blocks are not stored as blobs; they are recorded as `ZERO` runs in the manifest.

## Storage layout (agent-defined, driver-independent)
The agent defines the logical backup layout. Drivers only perform generic read/write operations on relative paths.

For filesystem storage, the files are stored under `backup.base_path/VirtBackup`:
```
backup.base_path/
  VirtBackup/
    blobs/
      <blockSizeMB>/
        ab/<sha256>
    manifests/
      <serverId>/
        <vmName>/
          <timestamp>.manifest.gz
    tmp/
```

Remote drivers (Google Drive/SFTP) use the same agent-defined relative layout under their own storage roots.

## Manifest format (text, gzipped)
One manifest is written per VM timestamp and stored as `<timestamp>.manifest.gz`.

Headers:
```
version: 1
block_size: 1048576
server_id: <serverId>
vm_name: <vmName>
timestamp: 2026-02-01T12:34:56.000Z
meta:
  format: inline_v1
  domain_xml_encoding: base64+gzip
  chain_encoding: yaml
domain_xml_b64_gz: |
  <base64-gzip payload>

disk_id: <diskId>
source_path: </remote/path/to/disk.qcow2>
file_size: <bytes>          (present if known)
chain:
  - order: 0
    disk_id: <diskId>
    path: </remote/path/to/disk.qcow2>
blocks: </remote/path/to/disk.qcow2>
```

Blocks (0-based):
```
0 -> <sha256>
1 -> <sha256>
6 -> ZERO
100-120 -> ZERO
EOF
```

Notes:
- `domain.xml` is embedded once per manifest.
- Disk metadata (`disk_id`, `source_path`, `chain`, `blocks`) is repeated per disk section.
- Restore requires an `EOF` terminator line to accept a manifest as complete.

## Backup flow (agent)
The agent creates one VM manifest and appends per-disk sections while the backup is running.

There are two paths:
1. **Hashblocks path (primair)**
   - The `hashblocks` binary is uploaded to the remote host and executed there.
   - The agent consumes block hashes (and ZERO runs) from the remote output.
   - For hashes: it checks blob existence via the driver; only missing blocks are fetched via SFTP range reads and written as blobs.
   - Backpressure: the agent sends `LIMIT` updates to hashblocks based on writer backlog.
2. **Stream fallback path**
   - If `hashblocks` is not available, the agent streams the entire disk file via SFTP.
   - The agent chunks using configured `blockSizeMB`, hashes locally, and writes only missing blobs.

## Restore flow (agent)
1. The agent resolves the correct `*.manifest.gz` for the selected VM timestamp.
2. The manifest is read; for each disk section and each block:
   - `ZERO` means "write zero bytes" for the block range.
   - A hash means "read blob `blobs/<blockSizeMB>/ab/<hash>` and write it to the output".
3. The last (tail) block length can be smaller than the configured block size based on `file_size`.

## Chain metadata
Chain metadata is embedded per disk section (`chain:`). No separate `*.chain` sidecar file is written.

## Concurrency / safety
- Blob writes: atomic "temp -> rename" per driver (filesystem and gdrive) to avoid races and partially-written blobs.
- Manifest: during backup a local `.inprogress` file is written; after backup the manifest is gzipped and uploaded as one file.

## Out of scope (intentionally)
- Garbage collection / refcounting for blobs (not implemented yet).
- Encryption / blob compression (not implemented; only manifests are gzipped).
