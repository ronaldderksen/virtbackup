# Destinations Acceptance Test Scenarios

## Purpose
This document defines end-to-end acceptance scenarios for destination behavior.

## Test data baseline
- Destinations:
- `filesystem` (enabled, valid path)
- `dest_sftp_a` (enabled, valid credentials A)
- `dest_sftp_b` (enabled, valid credentials B)
- `dest_gdrive` (enabled, valid refresh token)
- `dest_dummy` (enabled)
- one disabled destination for negative tests

- At least one VM available for backup.

## Scenario 1: Mandatory filesystem invariant

Steps:
1. Submit `/config` without a filesystem destination.
2. Read back `/config`.

Expected:
- `filesystem` destination exists and is enabled.
- filesystem path is present (empty allowed if not configured).

## Scenario 2: Backup uses explicit destinationId

Steps:
1. Start backup with `destinationId=dest_sftp_b`.
2. Observe job logs and connection target.

Expected:
- job starts successfully.
- effective driver is `sftp`.
- runtime settings reflect `dest_sftp_b` credentials, not `dest_sftp_a`.

## Scenario 3: Backup default destination fallback

Steps:
1. Set `backupDestinationId=dest_dummy`.
2. Start backup without `destinationId`.

Expected:
- selected destination resolves to `dest_dummy`.
- job runs on `dummy` driver.

## Scenario 4: Disabled destination rejection

Steps:
1. Disable `dest_sftp_b`.
2. Start backup with `destinationId=dest_sftp_b`.

Expected:
- request fails with destination unavailable error.

## Scenario 5: Restore entries destination scope

Steps:
1. Query `/restore/entries?destinationId=dest_sftp_a`.
2. Query `/restore/entries?destinationId=dest_gdrive`.

Expected:
- each result set is scoped to its destination/driver context.
- no cross-contamination of entries from other destinations.

## Scenario 6: Restore start with explicit destinationId

Steps:
1. Select XML from destination A.
2. Start restore with `destinationId` for destination A.

Expected:
- restore worker uses destination A settings.
- job starts and progresses without destination mismatch errors.

## Scenario 7: Unknown destinationId errors

Steps:
1. Start backup with missing destination id.
2. Start restore with missing destination id.
3. Query restore entries with missing destination id.

Expected:
- backup/restore start reject with destination-not-found error.
- restore entries returns empty list for unknown destination id.

## Scenario 8: Legacy driverId override compatibility

Steps:
1. Start backup with `destinationId=dest_sftp_a` and `driverId=dummy`.

Expected:
- request accepted (legacy compatibility).
- effective driver follows `driverId` override.
- note this as compatibility behavior with operational risk.

## Scenario 9: Secret encryption at rest

Steps:
1. Save config with plaintext `sftp password` and gdrive tokens.
2. Inspect persisted settings file on disk.
3. Read back runtime `/config`.

Expected:
- disk file stores encrypted fields (`passwordEnc`, token enc fields).
- plaintext secret fields are cleared in persisted representation.
- runtime config returns decrypted usable values where applicable.

## Scenario 10: Worker serialization integrity

Steps:
1. Start backup with `destinationId=dest_sftp_b`.
2. Confirm worker payload reconstructs correct SFTP settings.

Expected:
- `AppSettings.toMap()/fromMap()` round-trip keeps projected top-level SFTP fields.
- no fallback to first `sftp` destination.

## Scenario 11: GUI selection resilience

Steps:
1. Set selected destination in GUI to an enabled destination.
2. Disable/remove that destination.
3. Re-open backup tab.

Expected:
- GUI auto-selects:
1. existing valid selection, else
2. `backupDestinationId` if still enabled, else
3. first enabled destination

## Scenario 12: Filesystem path requirement enforcement

Steps:
1. Configure filesystem destination with empty path.
2. Start backup/restore using filesystem driver context.

Expected:
- backup start rejects with missing path when driver requires path.
- restore start transitions to failure state with backup path empty message.
