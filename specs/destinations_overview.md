# Destinations Overview Spec

## Purpose
This spec defines how storage destinations are modeled, persisted, selected, and propagated through backup and restore flows.

## Canonical model
A destination is represented by `BackupDestination` in `lib/common/models.dart`.

Fields:
- `id`: unique identifier used by API and UI selection.
- `name`: display label.
- `driverId`: storage driver (`filesystem`, `sftp`, `gdrive`, `dummy`, or custom/legacy id).
- `enabled`: controls eligibility for selection.
- `params`: driver-specific configuration map.

## Destination params by driver
Current expected params by built-in driver:

- `filesystem`
- `path` (string)

- `sftp`
- `host` (string)
- `port` (number)
- `username` (string)
- `password` (string, decrypted in-memory)
- `passwordEnc` (string, encrypted at rest)
- `basePath` (string)

- `gdrive`
- `rootPath` (string)
- `scope` (string)
- `accessToken` (string, decrypted in-memory)
- `refreshToken` (string, decrypted in-memory)
- `accessTokenEnc` (string, encrypted at rest)
- `refreshTokenEnc` (string, encrypted at rest)
- `accountEmail` (string)
- `expiresAt` (ISO timestamp)

## Settings-level fields related to destinations
`AppSettings` in `lib/common/settings.dart` stores:
- `destinations`
- `backupDestinationId`
- `restoreDestinationId`
- `backupDriverId`

It also stores projected driver runtime fields used by workers/drivers:
- SFTP projection: `sftpHost`, `sftpPort`, `sftpUsername`, `sftpPassword`, `sftpBasePath`
- GDrive projection: `gdriveScope`, `gdriveRootPath`, `gdriveAccessToken`, `gdriveRefreshToken`, `gdriveAccountEmail`, `gdriveExpiresAt`

## Invariants

- Filesystem destination is mandatory.
- `AppSettings.fromMap()` always ensures a destination with:
- `id = filesystem`
- `name = Filesystem`
- `driverId = filesystem`
- `enabled = true`

- If `backupDestinationId` is missing/empty, default backup destination is:
1. first enabled destination
2. otherwise first destination

- Destination parse rules:
- entries with empty `id` or empty `driverId` are dropped.
- `enabled` defaults to `true` unless explicitly `false`.

## Selection precedence (high level)

Backup and restore entry resolution use this order:
1. explicit `destinationId` request (API body or query)
2. configured `backupDestinationId` (for default backup destination)
3. first enabled destination
4. first destination

Driver selection can also be forced with legacy `driverId` override on job start.

## Compatibility and migration status

- Preferred model: destination-driven (`destinationId` + `destinations[*].params`).
- Legacy compatibility retained:
- backup/restore APIs still accept `driverId` and optional `driverParams`.
- some legacy fields under `backup.*` are still read by tooling fallback paths.

## Security at rest

`lib/agent/settings_store.dart` encrypts destination secrets:
- SFTP password to `destinations[*].params.passwordEnc`
- GDrive tokens to `destinations[*].params.accessTokenEnc` and `refreshTokenEnc`

At runtime, decrypted plain values are populated into `params.password`, `params.accessToken`, `params.refreshToken`.

## Runtime projection contract

Before spawning backup/restore workers, HTTP server resolves a concrete destination and projects destination params into an `AppSettings` instance (`_settingsForDestination`).

This projected settings object is serialized and sent to isolate workers. `AppSettings.toMap()` and `fromMap()` must preserve projected top-level SFTP/GDrive fields to avoid selecting credentials from unrelated destinations.

## Companion specs

- API examples: `specs/destinations_api_examples.md`
- Acceptance scenarios: `specs/destinations_acceptance_tests.md`
