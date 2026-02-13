# Destinations GUI and Tooling Spec

## Scope
This document specifies destination-related behavior in:
- GUI (`lib/gui/main_screen.dart`, `lib/gui/backup_tab.dart`, `lib/gui/service.dart`, `lib/gui/agent_api_client.dart`)
- CLI tooling (`tools/backup_verify.dart`)
- Public API expectations (`doc/api.md`)

## GUI destination list and selection

Enabled destination list (`_enabledDestinations`):
- includes only `enabled == true` destinations.
- ignores empty ids.
- de-duplicates by id.

Selection guard (`_ensureBackupDestinationSelection`):
1. keep current selection if still enabled.
2. else use settings `backupDestinationId` if enabled.
3. else use first enabled destination.
4. if no enabled destinations, selection is cleared.

## Destination management dialog behavior

The destination editor supports add/edit/delete with these rules:

- Driver is immutable for existing destinations.
- Filesystem destination cannot be deleted (`id=filesystem`).
- New destination id format:
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
- dialog builds `BackupDestination` objects with driver params.
- saves full updated settings through `POST /config`.
- updates `backupDestinationId` to current valid selection or first destination.

## Backup start from GUI

`_runVmBackup` requires:
- selected server with SSH connection
- non-empty base path field in settings view
- non-empty selected destination id

Backup API request (`AgentApiClient.startBackup`):
- `POST /servers/{id}/backup`
- payload includes `vmName` and required `destinationId`
- optional `driverParams` included if provided

## Tooling (`tools/backup_verify.dart`) destination behavior

Supported option:
- `--dest <id>` maps to `destinationId`.

Backup/restore calls:
- send `destinationId` in start backup and start restore requests.
- use `destinationId` in `/restore/entries` query.

Config introspection helper:
- resolves destination by id from `/config.destinations`.
- for filesystem, returns destination path.
- for non-filesystem, prints destination label summary.
- fallback path still checks legacy `backup.base_path` only for compatibility diagnostics.

## API contract summary (as consumed by GUI/tooling)

From `doc/api.md`:
- Backup start prefers `destinationId`.
- Restore start prefers `destinationId`.
- Restore entries accept `destinationId` query.
- Legacy `driverId` and `driverParams` accepted for compatibility.

## Destination UX assumptions and constraints

- Destination names are user-facing labels; ids are stable keys.
- Disabled destinations are not selectable for jobs.
- Filesystem destination is always present and enabled by settings normalization.
- GUI does not enforce cross-destination uniqueness of `name`, only practical uniqueness of `id` in enabled list processing.

## Operational caveats (current behavior)

- Because legacy `driverId` override remains allowed, a caller can force a driver different from the selected destination driver.
- GUI does not expose forced driver override; this only affects direct API/tool callers.
- `restoreDestinationId` exists in settings but backup/restore job start currently resolves from requested destination or backup default flow.
