# Destinations Backend Flow Spec

## Scope
This document specifies backend destination behavior in:
- `lib/agent/http_server.dart`
- `lib/common/settings.dart`
- `lib/agent/settings_store.dart`
- `lib/agent/backup_worker.dart`
- `lib/agent/restore_worker.dart`

## API entry points using destinations

Backup:
- `POST /servers/{serverId}/backup`
- Request supports `destinationId` (preferred), `driverId` (legacy override), and `driverParams` (legacy per-job params).

Restore:
- `POST /servers/{serverId}/restore/start`
- Request supports `destinationId` (preferred), `driverId` (legacy override).

Restore discovery:
- `GET /restore/entries?destinationId=...` or `?driverId=...`
- `destinationId` takes precedence.

## Destination resolution algorithm
Implemented by `_resolveBackupDestination(requestedDestinationId)`.

Rules:
1. Build candidate list from `enabled` destinations only.
2. If `requestedDestinationId` present:
- select exact id from enabled candidates.
- if not found, return `null` (API responds with destination-not-found error).
3. If no requested id:
- try `_agentSettings.backupDestinationId` among enabled candidates.
- fallback to first enabled candidate.

Returned object (`_ResolvedDestination`) contains:
- selected `BackupDestination`
- projected `AppSettings` (`_settingsForDestination`)
- `driverId`
- `backupPath`
- `driverParams` copied from destination params

## Backup path derivation
`_backupPathForDestination(destination)`:
- For `filesystem`: use destination `params.path`.
- For non-filesystem: use filesystem destination path (`id=filesystem`).

Implication:
- remote drivers still depend on local filesystem backup root for cache/log/layout support.

## Driver settings projection
`_settingsForDestination(destination)` maps destination params into top-level runtime settings.

SFTP projection:
- `backupDriverId = sftp`
- `sftpHost`, `sftpPort`, `sftpUsername`, `sftpPassword`, `sftpBasePath` from destination params.

GDrive projection:
- `backupDriverId = gdrive`
- `gdriveScope`, `gdriveRootPath`, `gdriveAccessToken`, `gdriveRefreshToken`, `gdriveAccountEmail`, `gdriveExpiresAt` from destination params.

Filesystem and dummy:
- only `backupDriverId` switched.

## Job start and worker payload contract

Backup job start (`_startBackupJob`):
- sends worker payload with:
- `driverId`
- `backupPath`
- `driverParams`
- `settings = (resolved destination settings or global settings).toMap()`

Restore job start (`_startRestoreJob`):
- sends worker payload with:
- `driverId`
- `backupPath`
- `settings = (resolved destination settings or global settings).toMap()`

Workers hydrate settings using `AppSettings.fromMap()` and instantiate drivers from those hydrated settings.

## Serialization guarantees required for correctness

Because isolate workers only receive serialized settings maps, these keys must round-trip correctly:
- destination list and ids (`destinations`, `backupDestinationId`, `restoreDestinationId`)
- projected top-level runtime keys (`backupDriverId`, SFTP fields, GDrive fields)

Current implementation in `AppSettings.toMap()/fromMap()` preserves these fields.

## Driver override behavior (legacy)

If request includes `driverId`, backend uses it even when destination has another driver.

Current effect:
- selected destination still determines projected settings and default driver params.
- forced driver can run with settings meant for another destination driver.

This is intentionally backward-compatible behavior but operationally risky.

## Driver validation behavior
From `_buildDriverRegistry(...).validateStart`:

- `filesystem`: no extra validation beyond non-empty backup path when `usesPath=true`.
- `dummy`: no extra validation.
- `gdrive`: requires non-empty refresh token.
- `sftp`: requires host, username, password, basePath.

API rejects start if validation fails.

## Persistence and encryption for destination secrets
`settings_store.dart` processing:

On save:
- encrypt destination SFTP password into `passwordEnc`, clear `password`.
- encrypt destination GDrive tokens into `accessTokenEnc`/`refreshTokenEnc`, clear plaintext tokens.

On load:
- decrypt destination SFTP/GDrive secrets back into plaintext params for runtime use.

## Restore entries behavior by destination

`_loadRestoreEntries(...)`:
- resolves destination if `destinationId` provided.
- otherwise may resolve default destination.
- derives driver and settings from resolution.
- lists XML/manifests through that driver context only.

This means restore entry visibility is destination-scoped when destination is selected.

## Error semantics

Destination-related request errors:
- unknown forced `driverId`: HTTP 400 + known ids.
- unknown/unavailable requested `destinationId`: HTTP 400.
- missing path for path-using driver: HTTP 400 on backup start, or failure state on restore start.
- failed driver validation: HTTP 400 with validation message.
