# Agent Overview

This document describes the Virtbackup agent in detail: its responsibilities, runtime flow, and how the main components collaborate.

## Purpose

The agent is a local service that performs VM backup and restore operations over SSH/SFTP, maintains job status, and exposes an HTTPS API for orchestration and monitoring. It is designed to:

- Discover VM inventory and overlay status on remote hypervisors.
- Create consistent VM backups using snapshots and deduplicated block storage.
- Restore VMs from manifests and blobs.
- Provide real-time progress via HTTP endpoints and server-sent events (SSE).
- Support both native SFTP (FFI) and Dart-based SSH/SFTP.

## High-Level Architecture

Key modules under `lib/agent`:

- `backup.dart`: Core backup orchestration and dedup logic.
- `backup_host.dart`: SSH/SFTP operations and native SFTP bindings.
- `http_server.dart`: HTTP API, job management, and SSE events.
- `settings_store.dart`: Persistent settings storage.
- `logging_config.dart`: Logging interval configuration.
- `drv/`: Backup drivers (filesystem, Google Drive, dummy).
- `workers/`: Logical worker components used by the hashblocks path.

## Data Flow Summary

### Backup

1. API call: `POST /servers/{id}/backup` (optional `driverId` in body to override the driver for this job only, plus optional `driverParams` map for driver-specific parameters).
2. `AgentHttpServer` creates a job and calls `_startBackupJob`.
3. `BackupAgent.runVmBackup` orchestrates the flow:
   - Validate state and start timers.
   - Ensure storage driver readiness.
   - Capture VM XML via `virsh dumpxml`.
   - Resolve disk chain information using `qemu-img info --backing-chain`.
   - If VM is running, create a snapshot and operate on overlays.
   - Stream disk data with deduplication.
   - Commit snapshot if applicable.
4. Progress is reported to job status, and summarized on completion.

### Restore

1. API call: `POST /servers/{id}/restore/start` (optional `driverId` in body to override the driver for this job only) or `POST /restore/sanity`.
2. `AgentHttpServer` resolves manifests and disk chains from stored metadata.
3. Restore stream uploads reconstructed data to remote disks via SFTP.
4. VM XML is uploaded and defined with `virsh define`.

### Events

- `GET /events` provides SSE for VM lifecycle changes and job events.
- `AgentHttpServer` maintains a persistent `virsh event --all --loop` listener.

## Backup Internals (Detailed)

### BackupAgent and Dependencies

`BackupAgent` is dependency-injected using `BackupAgentDependencies` which includes:

- SSH command execution.
- Disk path discovery.
- SFTP stream/range read and upload.
- Snapshot create/commit/cleanup.
- Hashblocks handling for dedup.

This keeps the backup core independent from transport details.

### Disk Planning

For each VM disk:

- Disk paths are loaded for both active and inactive states (`virsh domblklist --details`).
- The backing chain is discovered with `qemu-img info --backing-chain`.
- The chain is normalized to exclude internal `.virtbackup-` overlays.
- A backup plan is built as a sequence of chain items.

### Deduplication Pipeline

The backup uses a deduplication model based on fixed-size blocks:

- Block size: 1 MiB (1,048,576 bytes).
- Each block produces a SHA-256 hash.
- Manifests record hash or zero-runs for each block.
- Blobs are stored per hash and are only written if missing.

There are two paths:

1. **Hashblocks path (primary)**
   - A small helper binary (`hashblocks`) is uploaded to the remote host.
   - It streams hashes for the remote disk, and allows backpressure via `LIMIT`.
   - Missing blocks are fetched via SFTP range reads and written as blobs.

2. **Stream fallback path**
   - The agent streams the full disk through SFTP.
   - Hashes are computed locally for each block.
   - Blocks are written if missing.

### End-to-End Flow (Hashblocks Path)

1. **Backup start**
   - Agent selects the driver and prepares the destination.
   - The agent initializes an in-memory blob cache and performs an upfront scan (`shard1` -> `shard2` -> blob names) before hash checks start.
   - Manifests and folders are created as needed.

2. **Hashblocks starts**
   - The `hashblocks` binary is uploaded to the remote host.
   - It streams lines for each block:
     - `index -> <sha256>`
     - `start-end -> ZERO`
     - `EOF`

3. **Hashblocks processing**
   - Each line advances hashblocks progress (including ZERO/existing).
   - ZERO runs are written to the manifest (no SFTP).
   - For hashes, entries are pushed to:
     - blob-cache queue (directory hydration/ready)
     - exists queue (existing/missing decision)

4. **SFTP missing runs**
   - The SFTP worker fetches only missing ranges.
   - Bytes are pushed into the writer queue.

5. **Writer queue**
- `writerQueuedBytes`: waiting to be written
- `writerInFlightBytes`: currently being written
- `driverBufferedBytes`: buffered inside the driver (e.g. internal upload buffers)

6. **Hashblocks LIMIT**
   - LIMIT caps how far the remote hash stream may advance.
   - LIMIT is derived from progress + a buffer (`hashblocksLimitBufferMb`, default 1024 MB).
   - LIMIT updates at half-buffer intervals to avoid excessive commands.

7. **Driver writes**
   - GDrive: blobs are uploaded as individual files under `blobs/<shard1>/<shard2>/`.
   - SFTP: operations use retry with exponential backoff (2s, 4s, 8s, ...) on transient failures.
   - Filesystem: blobs are written directly.
   - Dummy: blob existence is simulated per driver rules.
   - The agent maintains a per-backup blob cache outside the drivers; write decisions are made by this cache.
   - Driver `writeBlob` paths are blind-write only (no exists/list/dir scans in the write call).
   - This is a strict invariant for all drivers; do not add safety checks into `writeBlob`.

8. **Finalize**
   - Manifests are committed and the job completes.

### Workers (Hashblocks Path)

The hashblocks path is divided into logical workers:

- `HashblocksWorker` (in `workers/hashblocks_worker.dart`)
  - Consumes lines from the hashblocks process.
  - Parses hashes and zero runs.
  - Pushes every hash to blob-cache and exists queue without blocking on exists checks.
  - Applies `LIMIT` backpressure when necessary.
- `BlobCacheWorker` (in `workers/blob_cache_worker.dart`)
  - Runs an initial `blobs` scan at startup via the active driver.
  - Prefetches shard directory listings based on incoming hashes.
  - Hash parsing does not wait for dirlist scans.
  - When a shard is missing, blob-cache performs shard creation via driver calls and updates shard-ready state.
  - Queue dequeue is immediate and independent: incoming hashes are dispatched fire-and-forget.
  - Worker wait is bounded (short timeout polling) to avoid missed wake stalls while queue work is pending.

- `ExistsWorker` (in `workers/exists_worker.dart`)
  - Receives all hashes through a dedicated queue.
  - Waits for shard-ready before deciding existing/missing.
  - Existing blocks are counted and marked as physically handled without SFTP fetch.
  - Missing hashes are grouped into contiguous runs and forwarded to `SftpWorker`.

- `SftpWorker` (in `workers/sftp_worker.dart`)
  - Fetches missing blocks via range reads.
  - Missing hashes are forwarded immediately (run size 1), then split into ranges.
  - Streams block data into the writer queue.

- `WriterWorker` (in `workers/writer_worker.dart`)
  - De-queues blocks and writes blobs.
  - Maintains continuous slot-based concurrency (no batch write loop).
  - Tracks backlog to apply backpressure.

### Backpressure Control

Backpressure is a combination of:

- Hashblocks `LIMIT`: throttles how far the remote hash stream advances.
- Writer backlog thresholds: throttles SFTP reads when queued/in-flight bytes plus driver-reported buffered bytes exceed limits.
- LIMIT is based on progress plus a configured buffer (`hashblocksLimitBufferMb`).
- LIMIT growth is paused only when writer backlog exceeds the configured backlog limit (default 4 GB).
- Writer concurrency: each driver sets its own max concurrent writes, which affects how fast the backlog drains.

### Progress Tracking

Progress is tracked via `BackupAgentProgress`:

- Logical bytes transferred (dedup-aware).
- Physical bytes written (actual blob storage).
- 30-second window speeds (logical) and physical upload throughput based on completed bytes.
- Disk counts and total bytes.
- Writer metrics: queued bytes, in-flight bytes, and driver-buffered bytes.

These are sampled by timers and pushed to the job status in the HTTP server.

## HTTP API (Summary)

Note: the agent serves HTTPS and requires authentication. See `doc/api.md` for the up-to-date endpoint contract.

Endpoints include:

- `GET /health`: liveness and native SFTP availability.
- `GET /drivers`: driver capabilities.
- `GET /config`, `POST /config`: settings.
- `POST /ntfyme/test`: send a test Ntfy me notification using the configured token.
- `GET /servers/{id}/vms`: VM inventory.
- `POST /servers/{id}/backup`: start backup job (supports optional `driverId` in the request body).
- `POST /servers/{id}/restore/start`: start restore job (supports optional `driverId` in the request body).
- `POST /restore/sanity`: validate manifests/blobs.
- `GET /jobs`, `GET /jobs/{id}`: job status.
- `POST /jobs/{id}/cancel`: cancel jobs.
- `GET /events`: SSE stream.
- `GET /restore/entries`: list restore candidates. Optional query `driverId` selects a specific storage driver.

## Drivers

Three drivers are present:

- `FilesystemBackupDriver`: stores manifests and blobs on disk.
- `GdriveBackupDriver`: stores manifests and blobs in Google Drive (with a local cache).
- `SftpBackupDriver`: stores manifests and blobs on an SFTP server (configured via `backup.sftp` in `agent.yaml`).
- `DummyBackupDriver`: discards writes and simulates ~20 MB/s write throughput (useful for testing backpressure).

Both implement the same `BackupDriver` interface.

## Native SFTP

The agent supports optional native SFTP via FFI:

- Shared library is loaded at runtime from `native/` directories.
- If loaded, native sessions are used for range reads and streaming.
- Session lifecycle is managed by `beginLargeTransferSession` and `endLargeTransferSession`.

## Error Handling and Cancellation

- Jobs can be canceled via API; cancellation propagates to backup logic.
- Most operations are wrapped with try/catch; failures update job status.
- Snapshot cleanup is attempted on failure to avoid dangling overlays.

## File Layout (Agent)

- `lib/agent/backup.dart`: core backup orchestration.
- `lib/agent/backup_types.dart`: public types and typedefs.
- `lib/agent/backup_models.dart`: internal helper models.
- `lib/agent/workers/*.dart`: worker components for hashblocks path.
- `lib/agent/backup_host.dart`: SSH/SFTP and native bindings.
- `lib/agent/http_server.dart`: HTTP API and job management.
- `lib/agent/settings_store.dart`: settings persistence.
- `lib/agent/logging_config.dart`: logging interval.

## Runtime Notes

- Backup and restore jobs run inside worker isolates to keep the HTTP listener responsive under heavy driver load (for example large Google Drive uploads).
- Default block size: 1 MiB (1,048,576 bytes).
- Progress sampling interval: `agentLogInterval` (currently 30s).
- Backup agent port: 33551.
- Uses SSH commands (`virsh`, `qemu-img`) on the remote host.
- The HTTP server runs over HTTPS with a self-signed certificate.
- The certificate and key are stored next to `agent.yaml` as `agent.crt` and `agent.key`.
- Certificates are generated for 10 years if missing; clients must trust or allow self-signed certs.
- The GUI prompts to trust and save the certificate fingerprint on first use, and warns if it changes.
- The HTTP API requires an auth token for every request via `Authorization: Bearer <token>` or `x-agent-token: <token>`.
- The token is generated on first run and stored next to `agent.yaml` as `agent.token` with owner-only permissions.
- The GUI reads the token from disk and attaches it to all agent requests.
- SSH passwords are encrypted at rest in `agent.yaml` using AES-GCM with a key derived from the token.
- Encrypted values are stored as `sshPasswordEnc` and decrypted into memory on load.
- Ntfy me notifications are sent by the agent when backup/restore jobs finish (success or failure).
- The agent posts JSON to `https://ntfyme.net/msg` with topic `virtbackup-job`.
- `driver` is always included and contains the driver label.
- `push_msg` is formatted as `<msg> on <driver label>`.
- `size` is included for backup jobs as a human-readable size (KiB/MiB/GiB).
- Set `ntfymeToken` in agent settings to enable notifications; when empty, notifications are skipped.
- The GUI can store multiple agent addresses and switch between them.
- For `127.0.0.1`, the GUI always uses the local `agent.token` file; other agents require a token entered in the GUI (token is mandatory).
- Google Drive OAuth client config is loaded from `etc/google_oauth_client.json` next to the executable or from `etc/google_oauth_client.json` under the current working directory (installed app client; the agent requires `client_secret` for the Drive driver).
- Google Drive OAuth refresh/access tokens are stored in `agent.yaml` as encrypted values under `backup.gdrive` (`accessTokenEnc`, `refreshTokenEnc`) using the same AES-GCM key derivation as SSH passwords.
- SFTP password is stored in `agent.yaml` as an encrypted value under `backup.sftp` (`passwordEnc`) using the same AES-GCM key derivation as SSH passwords.
- The Google Drive storage driver (`driverId: gdrive`) stores data as individual blob files using the same directory layout as the filesystem driver.
- All Google Drive API calls retry up to 5 times with exponential backoff starting at 2 seconds; each retry recreates the HTTP client connection, and a persistent failure aborts the backup with a clean error.
- Log records are routed by source: `agent` writes to `VirtBackup/logs/agent.log` and `gui` writes to `VirtBackup/logs/gui.log` under the configured backup base path.
- Agent log filtering reads `log_level` from `agent.yaml` (default `console` when missing/empty); GUI log filtering reads `log_level` from SharedPreferences (default `console` when missing/empty).
- Log records include `timestamp`, `level`, and `message`; source is used only for routing and is not included in the line payload.
- Log writes go through a centralized sequential queue writer with file locking to avoid interleaved/corrupted lines when multiple producers log concurrently.
- The writer timestamps records when they are enqueued; on slow storage the queue can lag while preserving record order.
- On each process startup, `agent.log` and `gui.log` are rotated to `<name>.log.1` and a fresh log file is started.
- `level=console` records are also echoed to stdout by the writer so console output and persisted logs stay aligned.
- SFTP debug logs include `action=sftp_timing` records with `leaseWaitMs`, `connectMs`, `opMs`, `leaseSource`, and `queued` to separate pool wait/connect time from operation time.
- Google Drive HTTP calls are logged as operation records with timestamp, action (`mkdir`, `list`, `upload`, `download`, `move`, `trash`, `auth.refresh`), status, duration, and request details.
- Google Drive upload HTTP clients are leased from a bounded pool so upload retries/concurrency cannot fan out into unbounded concurrent connections.
- Google Drive folder creation is guarded by a local folder lock and checks for existing folders before creating; when duplicate folder names are detected, the driver performs a strict merge into a primary folder and fails the operation if duplicates cannot be fully resolved.
- The filesystem backup path is configured via `backup.base_path` and the app creates and uses a `VirtBackup` folder inside that path.
- The Google Drive root folder is configured via `backup.gdrive.rootPath` (default `/`), and the app creates a `VirtBackup` folder inside that path.
- The SFTP base path is configured via `backup.sftp.basePath` (example `/Backup`), and the app creates and uses a `VirtBackup` folder inside that path.
