# Storages Overview Spec

## Purpose
This spec defines how storage storage are modeled, persisted, selected, and propagated through backup and restore flows.

## Canonical model
A storage is represented by `BackupStorage` in `lib/common/models.dart`.

Fields:
- `id`: unique identifier used by API and UI selection.
- `name`: display label.
- `driverId`: storage driver (`filesystem`, `sftp`, `gdrive`, `dummy`, or custom/legacy id).
- `enabled`: controls eligibility for selection.
- `params`: driver-specific configuration map.

## Storage params by driver
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

## Settings-level fields related to storage
`AppSettings` in `lib/common/settings.dart` stores:
- `storage`
- `backupStorageId`
- `restoreStorageId`
- `backupDriverId`

It also stores projected driver runtime fields used by workers/drivers:
- SFTP projection: `sftpHost`, `sftpPort`, `sftpUsername`, `sftpPassword`, `sftpBasePath`
- GDrive projection: `gdriveScope`, `gdriveRootPath`, `gdriveAccessToken`, `gdriveRefreshToken`, `gdriveAccountEmail`, `gdriveExpiresAt`

## Invariants

- Filesystem storage is mandatory.
- `AppSettings.fromMap()` always ensures a storage with:
- `id = filesystem`
- `name = Filesystem`
- `driverId = filesystem`
- `enabled = true`

- If `backupStorageId` is missing/empty, default backup storage is:
1. first enabled storage
2. otherwise first storage

- Storage parse rules:
- entries with empty `id` or empty `driverId` are dropped.
- `enabled` defaults to `true` unless explicitly `false`.

## Selection precedence (high level)

Backup and restore entry resolution use this order:
1. explicit `storageId` request (API body or query)
2. configured `backupStorageId` (for default backup storage)
3. first enabled storage
4. first storage

Driver selection can also be forced with legacy `driverId` override on job start.

## Compatibility and migration status

- Preferred model: storage-driven (`storageId` + `storage[*].params`).
- Legacy compatibility retained:
- backup/restore APIs still accept `driverId` and optional `driverParams`.
- some legacy fields under `backup.*` are still read by tooling fallback paths.

## Security at rest

`lib/agent/settings_store.dart` encrypts storage secrets:
- SFTP password to `storage[*].params.passwordEnc`
- GDrive tokens to `storage[*].params.accessTokenEnc` and `refreshTokenEnc`

At runtime, decrypted plain values are populated into `params.password`, `params.accessToken`, `params.refreshToken`.

## Runtime projection contract

Before spawning backup/restore workers, HTTP server resolves a concrete storage and projects storage params into an `AppSettings` instance (`_settingsForStorage`).

This projected settings object is serialized and sent to isolate workers. `AppSettings.toMap()` and `fromMap()` must preserve projected top-level SFTP/GDrive fields to avoid selecting credentials from unrelated storage.

## Companion specs

- API examples: `specs/storages_api_examples.md`
- Acceptance scenarios: `specs/storages_acceptance_tests.md`
