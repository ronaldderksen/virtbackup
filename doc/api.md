# Agent API (HTTPS)

Base URL (default): `https://127.0.0.1:33551/`

Notes:
- Payloads are JSON unless stated otherwise.
- The agent uses a self-signed TLS certificate generated on first start.
- All endpoints require an auth token (see **Authentication**).
- Agent config is stored on disk as YAML (`agent.yaml`).

## Authentication

Every request must include the agent token, either as:
- `Authorization: Bearer <token>`
- `x-agent-token: <token>`

If the token is missing/invalid the agent responds with:
```json
{"error":"unauthorized"}
```

## Health

- `GET /health`

Response:
```json
{"ok":true,"nativeSftpAvailable":true}
```

## Drivers

- `GET /drivers`

Response: array of available storage drivers and their capabilities/params.
```json
[
  {
    "id": "filesystem",
    "label": "Filesystem",
    "usesPath": true,
    "capabilities": {
      "supportsRangeRead": true,
      "supportsBatchDelete": true,
      "supportsMultipartUpload": false,
      "supportsServerSideCopy": false,
      "supportsConditionalWrite": false,
      "supportsVersioning": false,
      "maxConcurrentWrites": 16,
      "params": []
    }
  }
]
```

## Config

### Get full config

- `GET /config`

Response:
```json
{
  "destinations":[
    {
      "id":"dest_filesystem_1739440000000000",
      "name":"Local filesystem",
      "driverId":"filesystem",
      "enabled":true,
      "params":{"path":"/var"}
    },
    {
      "id":"dest_sftp_1739440000000001",
      "name":"Remote SFTP",
      "driverId":"sftp",
      "enabled":true,
      "params":{
        "host":"my-sftp.example.com",
        "port":22,
        "username":"backup",
        "password":"",
        "basePath":"/Backup"
      }
    }
  ],
  "backupDestinationId":"dest_filesystem_1739440000000000",
  "restoreDestinationId":null,
  "backup":{
    "driverId":"filesystem",
    "base_path":"/path/to/backups",
    "gdrive":{
      "scope":"https://www.googleapis.com/auth/drive.file",
      "rootPath":"/",
      "accessToken":"",
      "refreshToken":"",
      "accountEmail":"",
      "expiresAt":"2026-02-06T12:00:00Z"
    },
    "sftp":{
      "host":"my-sftp.example.com",
      "port":22,
      "username":"backup",
      "password":"",
      "basePath":"/Backup"
    }
  },
  "hashblocksLimitBufferMb":1024,
  "dummyDriverTmpWrites":false,
  "ntfymeToken":"",
  "connectionVerified":false,
  "selectedServerId":"server-id",
  "listenAll":false,
  "servers":[]
}
```

### Update full config

- `POST /config`

Body: same shape as `GET /config`.

Response:
```json
{"success":true}
```

## SFTP

### Test connection

- `POST /sftp/test`

Notes:
- The agent will create and use a `VirtBackup` folder inside `basePath`.

Body:
```json
{"host":"my-sftp.example.com","port":22,"username":"backup","password":"...","basePath":"/Backup"}
```

Response:
```json
{"success":true,"message":"SFTP connection successful (read/write OK)."}
```

## Events (SSE)

- `GET /events`

Response: `text/event-stream` (Server-Sent Events).

Initial event:
```
event: ready
data: ok
```

Subsequent events use:
- `event: <type>`
- `data: {"type":"<type>","payload":{...}}`

Currently emitted event types:
- `vm.lifecycle`
- `agent.job_failure`

## Notifications (Ntfy me)

- `POST /ntfyme/test`

Body (optional `token`; if missing, the agent uses `ntfymeToken` from config):
```json
{"token":"<ntfyme token>"}
```

Response (success):
```json
{"success":true,"message":"Test notification delivered.","statusCode":200}
```

## OAuth

### Store Google OAuth tokens

- `POST /oauth/google`

Body:
```json
{
  "accessToken":"<access token>",
  "refreshToken":"<refresh token>",
  "scope":"https://www.googleapis.com/auth/drive.file",
  "accountEmail":"user@example.com",
  "expiresAt":1775707200000
}
```

Notes:
- `refreshToken` is required.
- `expiresAt` may be a Unix timestamp in seconds or milliseconds, or an ISO-8601 string.

Response:
```json
{"success":true}
```

### Clear Google OAuth tokens

- `POST /oauth/google/clear`

Response:
```json
{"success":true}
```

## Servers

### List VM status (cached)

- `GET /servers/{serverId}/vms`

Response (array):
```json
[
  {
    "vm":{"id":"vm1","name":"vm1","powerState":"running"},
    "hasOverlay":false
  }
]
```

### Refresh server inventory (manual)

- `POST /servers/{serverId}/refresh`

Response:
```json
{"success":true}
```

### Test SSH connection

- `POST /servers/{serverId}/test`

Body:
```json
{}
```

Response:
```json
{"success":true}
```

### VM action

- `POST /servers/{serverId}/actions`

Body:
```json
{"action":"start|reboot|shutdown|forceReset|forceOff","vmName":"my-vm"}
```

Response:
```json
{"success":true}
```

### Cleanup overlays

- `POST /servers/{serverId}/cleanup`

Body:
```json
{"vmName":"my-vm"}
```

Response:
```json
{"success":true}
```

### Start backup job

- `POST /servers/{serverId}/backup`

Body:
```json
{
  "vmName":"my-vm",
  "destinationId":"dest_filesystem_1739440000000000",
  "fresh":false
}
```

Notes:
- `destinationId` selects one configured destination from root-level `destinations`.
- For backward compatibility, `driverId` and `driverParams` are still accepted when present.
- The filesystem destination path is resolved from `destinations[id=filesystem].params.path`.
- `fresh: true` triggers a driver "fresh cleanup" (driver-specific behavior). This is only executed when the agent runs in debug mode.

Response:
```json
{"jobId":"<job-id>"}
```

## Jobs

### List jobs

- `GET /jobs`

Response (array):
```json
[{"id":"<job-id>","type":"backup","state":"running","message":"","totalUnits":0,"completedUnits":0,"bytesTransferred":0,"speedBytesPerSec":0}]
```

### Get job status

- `GET /jobs/{jobId}`

Response (subset):
```json
{
  "id":"<job-id>",
  "type":"backup|restore|sanity",
  "state":"running|success|failure|canceled",
  "message":"...",
  "totalUnits":0,
  "completedUnits":0,
  "bytesTransferred":0,
  "speedBytesPerSec":0
}
```

Additional fields may be present (physical throughput, ETA, and writer backlog metrics).

### Cancel job

- `POST /jobs/{jobId}/cancel`

Response:
```json
{"success":true}
```

## Restore

### List restore entries

- `GET /restore/entries`

Optional query:
- `driverId=<id>`: list entries for a specific driver (otherwise uses the configured default driver).
- `destinationId=<id>`: list entries for a specific destination (takes precedence over `driverId`).

Response (array):
```json
[
  {
    "xmlPath":"/path/to/backup.xml",
    "vmName":"my-vm",
    "timestamp":"2026-01-30T12-00-00",
    "diskBasenames":["disk1.qcow2"],
    "missingDiskBasenames":[],
    "sourceServerId":"server-id",
    "sourceServerName":"Server"
  }
]
```

### Precheck

- `POST /servers/{serverId}/restore/precheck`

Body:
```json
{"xmlPath":"/path/to/backup.xml"}
```

Response:
```json
{"vmExists":true,"canDefineOnly":false}
```

### Start restore job

- `POST /servers/{serverId}/restore/start`

Body:
```json
{"xmlPath":"/path/to/backup.xml","decision":"overwrite|define","destinationId":"dest_filesystem_1739440000000000"}
```

Notes:
- `destinationId` selects one configured destination for restore reads.
- For backward compatibility, `driverId` is still accepted.

Response:
```json
{"jobId":"<job-id>"}
```

### Sanity check (verify manifests/blobs)

- `POST /restore/sanity`

Body:
```json
{"xmlPath":"/path/to/backup.xml","timestamp":"2026-01-30T12-00-00"}
```

Response:
```json
{"jobId":"<job-id>"}
```
