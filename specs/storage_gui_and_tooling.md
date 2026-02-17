# Storages GUI and Tooling Spec

## Scope
This document specifies storage-related behavior in:
- GUI (`lib/gui/main_screen.dart`, `lib/gui/backup_tab.dart`, `lib/gui/service.dart`, `lib/gui/agent_api_client.dart`)
- CLI tooling (`tools/backup_verify.dart`)
- Public API expectations (`doc/api.md`)

## GUI storage list and selection

Enabled storage list (`_enabledStorages`):
- includes only `enabled == true` storage.
- ignores empty ids.
- de-duplicates by id.

Selection guard (`_ensureBackupStorageSelection`):
1. keep current selection if still enabled.
2. else use settings `backupStorageId` if enabled.
3. else use first enabled storage.
4. if no enabled storage, selection is cleared.

## Storage management dialog behavior

The storage editor supports add/edit/delete with these rules:

- Driver is immutable for existing storage.
- Filesystem storage cannot be deleted (`id=filesystem`).
- New storage id format:
- `dest_<driverId>_<microsecondsSinceEpoch>`.

Driver-specific form fields:

- filesystem
- `Path` required

- sftp
- `Host` required
- `Port` optional (parsed to int, default 22)
- `Username` required
- `Password` required
- `Base path` required

- gdrive
- `Root path`, `Scope`, `Access token`, `Refresh token`, `Account email`

Save behavior:
- dialog builds `BackupStorage` objects with driver params.
- saves full updated settings through `POST /config`.
- updates `backupStorageId` to current valid selection or first storage.

## Backup start from GUI

`_runVmBackup` requires:
- selected server with SSH connection
- non-empty base path field in settings view
- non-empty selected storage id

Backup API request (`AgentApiClient.startBackup`):
- `POST /servers/{id}/backup`
- payload includes `vmName` and required `storageId`
- optional `driverParams` included if provided

## Tooling (`tools/backup_verify.dart`) storage behavior

Supported option:
- `--storage <id>` maps to `storageId`.

Backup/restore calls:
- send `storageId` in start backup and start restore requests.
- use `storageId` in `/restore/entries` query.

Config introspection helper:
- resolves storage by id from `/config.storage`.
- for filesystem, returns storage path.
- for non-filesystem, prints storage label summary.
- fallback path still checks legacy `backup.base_path` only for compatibility diagnostics.

## API contract summary (as consumed by GUI/tooling)

From `doc/api.md`:
- Backup start prefers `storageId`.
- Restore start prefers `storageId`.
- Restore entries accept `storageId` query.
- Legacy `driverId` and `driverParams` accepted for compatibility.

## Storage UX assumptions and constraints

- Storage names are user-facing labels; ids are stable keys.
- Disabled storage are not selectable for jobs.
- Filesystem storage is always present and enabled by settings normalization.
- GUI does not enforce cross-storage uniqueness of `name`, only practical uniqueness of `id` in enabled list processing.

## Operational caveats (current behavior)

- Because legacy `driverId` override remains allowed, a caller can force a driver different from the selected storage driver.
- GUI does not expose forced driver override; this only affects direct API/tool callers.
- `restoreStorageId` exists in settings but backup/restore job start currently resolves from requested storage or backup default flow.
