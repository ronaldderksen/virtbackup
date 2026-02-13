# Destinations API Examples Spec

## Purpose
This document provides concrete request/response examples for destination-driven flows.

## Preconditions
- Agent config contains at least:
- filesystem destination (`id=filesystem`)
- one enabled non-filesystem destination (for example `dest_sftp`)

## GET /config

Request:
```http
GET /config
Authorization: Bearer <token>
```

Response example (trimmed):
```json
{
  "destinations": [
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
  "backupDestinationId": "dest_sftp",
  "restoreDestinationId": null,
  "backupDriverId": "sftp"
}
```

## POST /servers/{id}/backup

### Success with destinationId

Request:
```json
{
  "vmName": "vm-prod-01",
  "destinationId": "dest_sftp"
}
```

Response:
```json
{
  "jobId": "1739999999999-backup"
}
```

### Success with destinationId + driverParams (legacy path still accepted)

Request:
```json
{
  "vmName": "vm-prod-01",
  "destinationId": "dest_dummy",
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

### Error unknown destinationId

Request:
```json
{
  "vmName": "vm-prod-01",
  "destinationId": "missing_destination"
}
```

Response:
```json
{
  "error": "destination not found or unavailable"
}
```

### Error unknown driverId override

Request:
```json
{
  "vmName": "vm-prod-01",
  "destinationId": "dest_sftp",
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

### Default destination scope

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

### Destination override

Request:
```http
GET /restore/entries?destinationId=dest_sftp
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

### Destination takes precedence over driverId

Request:
```http
GET /restore/entries?destinationId=dest_sftp&driverId=filesystem
```

Expected behavior:
- backend resolves using `destinationId=dest_sftp`
- `driverId` query is ignored for effective selection in this case

## POST /servers/{id}/restore/start

### Success with destinationId

Request:
```json
{
  "xmlPath": "/cache/sftp/manifests/server-a/vm-prod-01/2026-02-13T10-00-00__domain.xml",
  "decision": "overwrite",
  "destinationId": "dest_sftp"
}
```

Response:
```json
{
  "jobId": "1739999999999-restore"
}
```

### Error unknown destinationId

Request:
```json
{
  "xmlPath": "/x.xml",
  "decision": "overwrite",
  "destinationId": "missing_destination"
}
```

Response:
```json
{
  "error": "destination not found or unavailable"
}
```

## POST /config (destination persistence)

Request example (trimmed):
```json
{
  "destinations": [
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
  "backupDestinationId": "dest_sftp",
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
