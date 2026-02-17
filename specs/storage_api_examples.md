# Storages API Examples Spec

## Purpose
This document provides concrete request/response examples for storage-driven flows.

## Preconditions
- Agent config contains at least:
- filesystem storage (`id=filesystem`)
- one enabled non-filesystem storage (for example `dest_sftp`)

## GET /config

Request:
```http
GET /config
Authorization: Bearer <token>
```

Response example (trimmed):
```json
{
  "storage": [
    {
      "id": "filesystem",
      "name": "Filesystem",
      "driverId": "filesystem",
      "enabled": true,
      "params": {
        "path": "/var/backups"
      }
    },
    {
      "id": "dest_sftp",
      "name": "SFTP",
      "driverId": "sftp",
      "enabled": true,
      "params": {
        "host": "192.168.12.120",
        "port": 22,
        "username": "sftp",
        "password": "",
        "passwordEnc": "v1:..."
      }
    }
  ],
  "backupStorageId": "dest_sftp",
  "restoreStorageId": null,
  "backupDriverId": "sftp"
}
```

## POST /servers/{id}/backup

### Success with storageId

Request:
```json
{
  "vmName": "vm-prod-01",
  "storageId": "dest_sftp"
}
```

Response:
```json
{
  "jobId": "1739999999999-backup"
}
```

### Success with storageId + driverParams (legacy path still accepted)

Request:
```json
{
  "vmName": "vm-prod-01",
  "storageId": "dest_dummy",
  "driverParams": {
    "throttleMbps": 50
  }
}
```

Response:
```json
{
  "jobId": "1739999999999-backup"
}
```

### Error unknown storageId

Request:
```json
{
  "vmName": "vm-prod-01",
  "storageId": "missing_storage"
}
```

Response:
```json
{
  "error": "storage not found or unavailable"
}
```

### Error unknown driverId override

Request:
```json
{
  "vmName": "vm-prod-01",
  "storageId": "dest_sftp",
  "driverId": "unknown_driver"
}
```

Response:
```json
{
  "error": "unknown driverId",
  "known": ["filesystem", "dummy", "gdrive", "sftp"]
}
```

## GET /restore/entries

### Default storage scope

Request:
```http
GET /restore/entries
```

Response example:
```json
[
  {
    "xmlPath": "/var/backups/VirtBackup/manifests/server-a/vm-prod-01/2026-02-13T10-00-00__domain.xml",
    "vmName": "vm-prod-01",
    "timestamp": "2026-02-13T10-00-00",
    "diskBasenames": ["disk0.qcow2"],
    "missingDiskBasenames": [],
    "sourceServerId": "server-a",
    "sourceServerName": "Server A"
  }
]
```

### Storage override

Request:
```http
GET /restore/entries?storageId=dest_sftp
```

Response:
```json
[
  {
    "xmlPath": "/cache/sftp/manifests/server-a/vm-prod-01/2026-02-13T10-00-00__domain.xml",
    "vmName": "vm-prod-01",
    "timestamp": "2026-02-13T10-00-00",
    "diskBasenames": ["disk0.qcow2"],
    "missingDiskBasenames": [],
    "sourceServerId": "server-a",
    "sourceServerName": "Server A"
  }
]
```

### Storage takes precedence over driverId

Request:
```http
GET /restore/entries?storageId=dest_sftp&driverId=filesystem
```

Expected behavior:
- backend resolves using `storageId=dest_sftp`
- `driverId` query is ignored for effective selection in this case

## POST /servers/{id}/restore/start

### Success with storageId

Request:
```json
{
  "xmlPath": "/cache/sftp/manifests/server-a/vm-prod-01/2026-02-13T10-00-00__domain.xml",
  "decision": "overwrite",
  "storageId": "dest_sftp"
}
```

Response:
```json
{
  "jobId": "1739999999999-restore"
}
```

### Error unknown storageId

Request:
```json
{
  "xmlPath": "/x.xml",
  "decision": "overwrite",
  "storageId": "missing_storage"
}
```

Response:
```json
{
  "error": "storage not found or unavailable"
}
```

## POST /config (storage persistence)

Request example (trimmed):
```json
{
  "storage": [
    {
      "id": "filesystem",
      "name": "Filesystem",
      "driverId": "filesystem",
      "enabled": true,
      "params": {
        "path": "/var/backups"
      }
    },
    {
      "id": "dest_sftp",
      "name": "SFTP",
      "driverId": "sftp",
      "enabled": true,
      "params": {
        "host": "192.168.12.120",
        "port": 22,
        "username": "sftp",
        "password": "secret",
        "basePath": "/Backup"
      }
    }
  ],
  "backupStorageId": "dest_sftp",
  "backupDriverId": "sftp"
}
```

Response:
```json
{
  "success": true
}
```

Postcondition:
- secrets are stored encrypted at rest (`passwordEnc`, token enc fields)
- plaintext secrets are restored in runtime config after decrypt on load
