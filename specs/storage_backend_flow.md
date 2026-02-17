# Storages Backend Flow Spec

## Scope
This document specifies backend storage behavior in:
- `lib/agent/http_server.dart`
- `lib/common/settings.dart`
- `lib/agent/settings_store.dart`
- `lib/agent/backup_worker.dart`
- `lib/agent/restore_worker.dart`

## API entry points using storage

Backup:
- `POST /servers/{serverId}/backup`
- Request supports `storageId` (preferred), `driverId` (legacy override), and `driverParams` (legacy per-job params).

Restore:
- `POST /servers/{serverId}/restore/start`
- Request supports `storageId` (preferred), `driverId` (legacy override).

Restore discovery:
- `GET /restore/entries?storageId=...` or `?driverId=...`
- `storageId` takes precedence.

## Storage resolution algorithm
Implemented by `_resolveBackupStorage(requestedStorageId)`.

Rules:
1. Build candidate list from `enabled` storage only.
2. If `requestedStorageId` present:
- select exact id from enabled candidates.
- if not found, return `null` (API responds with storage-not-found error).
3. If no requested id:
- try `_agentSettings.backupStorageId` among enabled candidates.
- fallback to first enabled candidate.

Returned object (`_ResolvedStorage`) contains:
- selected `BackupStorage`
- projected `AppSettings` (`_settingsForStorage`)
- `driverId`
- `backupPath`
- `driverParams` copied from storage params

## Backup path derivation
`_backupPathForStorage(storage)`:
- For `filesystem`: use storage `params.path`.
- For non-filesystem: use filesystem storage path (`id=filesystem`).

Implication:
- remote drivers still depend on local filesystem backup root for cache/log/layout support.

## Driver settings projection
`_settingsForStorage(storage)` maps storage params into top-level runtime settings.

SFTP projection:
- `backupDriverId = sftp`
- `sftpHost`, `sftpPort`, `sftpUsername`, `sftpPassword`, `sftpBasePath` from storage params.

GDrive projection:
- `backupDriverId = gdrive`
- `gdriveScope`, `gdriveRootPath`, `gdriveAccessToken`, `gdriveRefreshToken`, `gdriveAccountEmail`, `gdriveExpiresAt` from storage params.

Filesystem and dummy:
- only `backupDriverId` switched.

## Job start and worker payload contract

Backup job start (`_startBackupJob`):
- sends worker payload with:
- `driverId`
- `backupPath`
- `driverParams`
- `settings = (resolved storage settings or global settings).toMap()`

Restore job start (`_startRestoreJob`):
- sends worker payload with:
- `driverId`
- `backupPath`
- `settings = (resolved storage settings or global settings).toMap()`

Workers hydrate settings using `AppSettings.fromMap()` and instantiate drivers from those hydrated settings.

## Serialization guarantees required for correctness

Because isolate workers only receive serialized settings maps, these keys must round-trip correctly:
- storage list and ids (`storage`, `backupStorageId`, `restoreStorageId`)
- projected top-level runtime keys (`backupDriverId`, SFTP fields, GDrive fields)

Current implementation in `AppSettings.toMap()/fromMap()` preserves these fields.

## Driver override behavior (legacy)

If request includes `driverId`, backend uses it even when storage has another driver.

Current effect:
- selected storage still determines projected settings and default driver params.
- forced driver can run with settings meant for another storage driver.

This is intentionally backward-compatible behavior but operationally risky.

## Driver validation behavior
From `_buildDriverRegistry(...).validateStart`:

- `filesystem`: no extra validation beyond non-empty backup path when `usesPath=true`.
- `dummy`: no extra validation.
- `gdrive`: requires non-empty refresh token.
- `sftp`: requires host, username, password, basePath.

API rejects start if validation fails.

## Persistence and encryption for storage secrets
`settings_store.dart` processing:

On save:
- encrypt storage SFTP password into `passwordEnc`, clear `password`.
- encrypt storage GDrive tokens into `accessTokenEnc`/`refreshTokenEnc`, clear plaintext tokens.

On load:
- decrypt storage SFTP/GDrive secrets back into plaintext params for runtime use.

## Restore entries behavior by storage

`_loadRestoreEntries(...)`:
- resolves storage if `storageId` provided.
- otherwise may resolve default storage.
- derives driver and settings from resolution.
- lists XML/manifests through that driver context only.

This means restore entry visibility is storage-scoped when storage is selected.

## Error semantics

Storage-related request errors:
- unknown forced `driverId`: HTTP 400 + known ids.
- unknown/unavailable requested `storageId`: HTTP 400.
- missing path for path-using driver: HTTP 400 on backup start, or failure state on restore start.
- failed driver validation: HTTP 400 with validation message.
